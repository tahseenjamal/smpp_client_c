#include <arpa/inet.h>
#include <nats/nats.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include "../common/config.h"

int publish_event(natsConnection* conn, const char* subject,
                  const char* payload);

#define MAX_SEQ 10000
#define MAX_WORKERS 16
#define MAX_BODY 4096
#define BATCH_SIZE 100
#define WAIT_TIMEOUT 1000

/* ---------------- Global TPS limiter ---------------- */

static int global_tps = 0;
static long send_interval_us = 0;

static pthread_mutex_t tps_lock = PTHREAD_MUTEX_INITIALIZER;
static long next_send_time_us = 0;

/* ---------------- Shutdown flag ---------------- */

static volatile sig_atomic_t running = 1;
static void on_signal(int sig) { (void)sig; running = 0; }

/* ---------------- Window semaphore (per-worker in-flight PDU cap) ---------- */

typedef struct {
    int             count;
    int             max;
    pthread_mutex_t lock;
    pthread_cond_t  cond;
} WindowSem;

static void wsem_init(WindowSem* ws, int max) {
    ws->count = max > 0 ? max : 1;
    ws->max   = ws->count;
    pthread_mutex_init(&ws->lock, NULL);
    pthread_cond_init(&ws->cond,  NULL);
}
static void wsem_wait(WindowSem* ws) {
    pthread_mutex_lock(&ws->lock);
    while (ws->count == 0)
        pthread_cond_wait(&ws->cond, &ws->lock);
    ws->count--;
    pthread_mutex_unlock(&ws->lock);
}
static void wsem_post(WindowSem* ws) {
    pthread_mutex_lock(&ws->lock);
    if (ws->count < ws->max) ws->count++;
    pthread_cond_signal(&ws->cond);
    pthread_mutex_unlock(&ws->lock);
}
/* Restore full capacity on reconnect so blocked senders unblock. */
static void wsem_reset(WindowSem* ws) {
    pthread_mutex_lock(&ws->lock);
    ws->count = ws->max;
    pthread_cond_broadcast(&ws->cond);
    pthread_mutex_unlock(&ws->lock);
}

/* ---------------- Prometheus metrics -------------------------------- */

static _Atomic long m_submit_ok  = 0;
static _Atomic long m_submit_err = 0;
static _Atomic long m_dlr_total  = 0;
static _Atomic long m_sessions   = 0;

#define RTT_BUCKET_N 9
static const long rtt_limits_ms[RTT_BUCKET_N] = {10,25,50,100,250,500,1000,2500,5000};
static _Atomic long rtt_bucket[RTT_BUCKET_N + 1]; /* +Inf is last entry */
static _Atomic long rtt_sum_ms = 0;
static _Atomic long rtt_count  = 0;

static void rtt_observe(long ms) {
    atomic_fetch_add(&rtt_sum_ms, ms);
    atomic_fetch_add(&rtt_count, 1);
    for (int i = 0; i < RTT_BUCKET_N; i++)
        if (ms <= rtt_limits_ms[i])
            atomic_fetch_add(&rtt_bucket[i], 1);
    atomic_fetch_add(&rtt_bucket[RTT_BUCKET_N], 1); /* +Inf always */
}

/* ------------------------------------------------ */

typedef struct {
    char txid[64];
    char text[512];
    long submit_us; /* wall-clock µs when submit_sm was written to socket */
} SeqMap;

typedef struct {
    int id;
    int socket;
    pthread_t thread;

    pthread_mutex_t lock;        /* protects seq_map */
    pthread_mutex_t socket_lock; /* protects socket fd during reconnect */

    WindowSem window;            /* limits in-flight PDUs to smpp_window_size */

    SeqMap seq_map[MAX_SEQ];
    uint32_t seq;
} SmppWorker;

SmppWorker workers[MAX_WORKERS];

natsConnection* nc = NULL;

int worker_count = 1;
int rr = 0;

/* ------------------------------------------------ */

static long now_us() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (tv.tv_sec * 1000000L) + tv.tv_usec;
}

/* ------------------------------------------------ */

static void global_tps_wait() {
    if (send_interval_us <= 0) return;

    pthread_mutex_lock(&tps_lock);

    long now = now_us();

    if (next_send_time_us == 0) next_send_time_us = now;

    if (now < next_send_time_us) {
        long sleep_us = next_send_time_us - now;
        pthread_mutex_unlock(&tps_lock);
        struct timespec ts;
        ts.tv_sec = sleep_us / 1000000;
        ts.tv_nsec = (sleep_us % 1000000) * 1000;
        nanosleep(&ts, NULL);
        pthread_mutex_lock(&tps_lock);
    }

    now = now_us();

    if (now > next_send_time_us)
        next_send_time_us = now + send_interval_us;
    else
        next_send_time_us += send_interval_us;

    pthread_mutex_unlock(&tps_lock);
}

/* ------------------------------------------------ */

int read_full(int sock, void* buf, int len) {
    int total = 0;

    while (total < len) {
        int r = read(sock, (char*)buf + total, len - total);
        if (r <= 0) return -1;

        total += r;
    }

    return total;
}

/* ------------------------------------------------ */

void extract_json(const char* json, const char* key, char* out, int max) {
    char pattern[64];

    snprintf(pattern, sizeof(pattern), "\"%s\":\"", key);

    char* p = strstr(json, pattern);
    if (!p) return;

    p += strlen(pattern);

    char* end = strchr(p, '"');
    if (!end) return;

    int len = end - p;

    if (len >= max) len = max - 1;

    memcpy(out, p, len);
    out[len] = 0;
}

/* ------------------------------------------------ */

uint32_t next_seq(SmppWorker* w) { return ++w->seq; }

/* ------------------------------------------------ */

void write_u32(uint8_t* buf, int offset, uint32_t value) {
    uint32_t v = htonl(value);
    memcpy(buf + offset, &v, 4);
}

/* ------------------------------------------------ */

void read_u32(uint8_t* buf, int offset, uint32_t* out) {
    memcpy(out, buf + offset, 4);
    *out = ntohl(*out);
}

/* ------------------------------------------------ */

void write_cstring(uint8_t* buf, int* pos, const char* str) {
    int len = strlen(str);

    memcpy(buf + *pos, str, len);
    *pos += len;

    buf[(*pos)++] = 0;
}

/* ------------------------------------------------ */

int smpp_connect(SmppWorker* w) {
    struct sockaddr_in addr;

    int sock = socket(AF_INET, SOCK_STREAM, 0);

    if (sock < 0) {
        perror("socket");
        return -1;
    }

    addr.sin_family = AF_INET;
    addr.sin_port = htons(config.smpp_port);
    addr.sin_addr.s_addr = inet_addr(config.smpp_host);

    if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("SMPP connect");
        close(sock);
        return -1;
    }

    pthread_mutex_lock(&w->socket_lock);
    w->socket = sock;
    pthread_mutex_unlock(&w->socket_lock);

    printf("[SMPP] Worker %d connected\n", w->id);

    return 0;
}

/* ------------------------------------------------ */

void smpp_bind(SmppWorker* w) {
    uint8_t pdu[1024];
    int pos = 16;

    write_cstring(pdu, &pos, config.smpp_system_id);
    write_cstring(pdu, &pos, config.smpp_password);
    write_cstring(pdu, &pos, config.smpp_system_type);

    pdu[pos++] = 0x34;
    pdu[pos++] = 0;
    pdu[pos++] = 0;
    pdu[pos++] = 0;

    uint32_t seq = next_seq(w);

    write_u32(pdu, 0, pos);
    write_u32(pdu, 4, 0x00000009);
    write_u32(pdu, 8, 0);
    write_u32(pdu, 12, seq);

    if (write(w->socket, pdu, pos) < 0) perror("[SMPP] bind write");

    printf("[SMPP] Worker %d bind_transceiver seq=%u\n", w->id, seq);
}

/* ------------------------------------------------ */
/* Marketing send window                            */
/* ------------------------------------------------ */

/*
 * Returns 1 if the current local time falls within [morning, evening].
 * morning / evening are "HH:MM" strings (e.g. "08:00", "21:00").
 * Returns 1 (open) when either string is empty or unparseable.
 */
static int within_send_window(const char* morning, const char* evening) {
    if (!morning[0] || !evening[0]) return 1;

    int mh = 0, mm = 0, eh = 0, em = 0;
    if (sscanf(morning, "%d:%d", &mh, &mm) != 2) return 1;
    if (sscanf(evening, "%d:%d", &eh, &em) != 2) return 1;

    time_t t     = time(NULL);
    struct tm* lt = localtime(&t);
    int now_mins = lt->tm_hour * 60 + lt->tm_min;

    return now_mins >= (mh * 60 + mm) && now_mins <= (eh * 60 + em);
}

/* ------------------------------------------------ */
/* Long message (concatenated SMS) support          */
/* ------------------------------------------------ */

#define UDH_SIZE     6
#define GSM_SINGLE   160
#define GSM_SEGMENT  153
#define UCS2_SINGLE  70
#define UCS2_SEGMENT 67
#define MAX_SEGMENTS 16

static uint8_t  ref_counter = 0;
static pthread_mutex_t ref_lock = PTHREAD_MUTEX_INITIALIZER;

/* Returns 1 if every byte is printable ASCII (0x20–0x7E or common controls). */
static int is_ascii(const char* msg) {
    for (const unsigned char* p = (const unsigned char*)msg; *p; p++)
        if (*p > 0x7E) return 0;
    return 1;
}

/*
 * Convert UTF-8 to UCS-2 big-endian.
 * Code points > U+FFFF are replaced with U+FFFD (can't fit in UCS-2).
 * Returns the number of UCS-2 code units written into dst.
 */
static int utf8_to_ucs2(const char* src, uint8_t* dst, int max_units) {
    const unsigned char* p = (const unsigned char*)src;
    int n = 0;

    while (*p && n < max_units) {
        uint32_t cp;

        if (*p < 0x80) {
            cp = *p++;
        } else if ((*p & 0xE0) == 0xC0 && (p[1] & 0xC0) == 0x80) {
            cp  = (uint32_t)(*p++ & 0x1F) << 6;
            cp |= *p++ & 0x3F;
        } else if ((*p & 0xF0) == 0xE0 && (p[1] & 0xC0) == 0x80 &&
                   (p[2] & 0xC0) == 0x80) {
            cp  = (uint32_t)(*p++ & 0x0F) << 12;
            cp |= (uint32_t)(*p++ & 0x3F) << 6;
            cp |= *p++ & 0x3F;
        } else {
            cp = 0xFFFD; /* replacement character */
            p++;
            while ((*p & 0xC0) == 0x80) p++; /* skip continuation bytes */
        }

        if (cp > 0xFFFF) cp = 0xFFFD;

        dst[n * 2]     = (cp >> 8) & 0xFF;
        dst[n * 2 + 1] =  cp       & 0xFF;
        n++;
    }
    return n;
}

/* ------------------------------------------------ */

/*
 * Build and write a single submit_sm PDU.
 * msg_data / msg_len are the raw bytes for the short_message field
 * (caller prepends UDH when splitting).
 * full_text is stored in seq_map so the submit event carries the
 * original message text.
 * Returns the sequence number used, or 0 if the socket is down.
 */
static uint32_t smpp_send_pdu(SmppWorker* w, const char* from, const char* to,
                               uint8_t data_coding, uint8_t esm_class,
                               const uint8_t* msg_data, int msg_len,
                               const char* txid, const char* full_text) {
    uint8_t pdu[1024]; /* 16 hdr + 2 addr fields(≤32B each) + 159B max segment + margin */
    int pos = 16;

    pdu[pos++] = 0;             /* service_type (empty C-string) */
    pdu[pos++] = 1;             /* source_addr_ton  */
    pdu[pos++] = 1;             /* source_addr_npi  */
    write_cstring(pdu, &pos, from);
    pdu[pos++] = 1;             /* dest_addr_ton    */
    pdu[pos++] = 1;             /* dest_addr_npi    */
    write_cstring(pdu, &pos, to);
    pdu[pos++] = esm_class;     /* esm_class (0x40 when UDH present) */
    pdu[pos++] = 0;             /* protocol_id      */
    pdu[pos++] = 0;             /* priority_flag    */
    pdu[pos++] = 0;             /* schedule_delivery_time (empty) */
    pdu[pos++] = 0;             /* validity_period  (empty) */
    pdu[pos++] = 1;             /* registered_delivery — request DLR */
    pdu[pos++] = 0;             /* replace_if_present */
    pdu[pos++] = data_coding;   /* 0x00 GSM7, 0x08 UCS-2 */
    pdu[pos++] = 0;             /* sm_default_msg_id */
    pdu[pos++] = (uint8_t)msg_len;
    memcpy(pdu + pos, msg_data, msg_len);
    pos += msg_len;

    /* Acquire a window slot — blocks if smpp_window_size PDUs are in flight. */
    wsem_wait(&w->window);

    pthread_mutex_lock(&w->socket_lock);
    int sock = w->socket;
    pthread_mutex_unlock(&w->socket_lock);

    if (sock < 0) {
        printf("[SMPP] Worker %d not connected, dropping submit\n", w->id);
        wsem_post(&w->window); /* give back the slot immediately */
        atomic_fetch_add(&m_submit_err, 1);
        return 0;
    }

    uint32_t seq = next_seq(w);
    write_u32(pdu, 0,  pos);
    write_u32(pdu, 4,  0x00000004);
    write_u32(pdu, 8,  0);
    write_u32(pdu, 12, seq);

    int idx = seq % MAX_SEQ;
    pthread_mutex_lock(&w->lock);
    snprintf(w->seq_map[idx].txid, sizeof(w->seq_map[idx].txid), "%s", txid);
    snprintf(w->seq_map[idx].text, sizeof(w->seq_map[idx].text), "%s", full_text);
    w->seq_map[idx].submit_us = now_us();
    pthread_mutex_unlock(&w->lock);

    if (write(sock, pdu, pos) < 0) {
        perror("[SMPP] submit write");
        wsem_post(&w->window);
        atomic_fetch_add(&m_submit_err, 1);
        return 0; /* 0 = definite failure: caller can NAK */
    }

    /* m_submit_ok is counted in the submit_sm_resp handler once SMSC confirms. */
    return seq;
}

/* ------------------------------------------------ */

/*
 * Submit an SMS message, automatically handling:
 *   - Single GSM 7-bit  (ASCII, ≤160 chars)
 *   - Multi-part GSM 7-bit  (ASCII, >160 chars — up to MAX_SEGMENTS×153)
 *   - Single UCS-2  (non-ASCII, ≤70 chars)
 *   - Multi-part UCS-2  (non-ASCII, >70 chars — up to MAX_SEGMENTS×67)
 *
 * For multi-part messages a 6-byte UDH is prepended to each segment and
 * esm_class bit 6 (UDHI) is set.  All segments share the same ref byte.
 */
uint32_t smpp_submit(SmppWorker* w, const char* from, const char* to,
                     const char* msg, const char* txid) {
    if (is_ascii(msg)) {
        int len = (int)strlen(msg);

        if (len <= GSM_SINGLE) {
            return smpp_send_pdu(w, from, to, 0x00, 0x00,
                                 (const uint8_t*)msg, len, txid, msg);
        }

        /* ---- multi-part GSM 7-bit ---- */
        uint8_t ref;
        pthread_mutex_lock(&ref_lock);
        ref = ref_counter++;
        pthread_mutex_unlock(&ref_lock);

        int total = (len + GSM_SEGMENT - 1) / GSM_SEGMENT;
        if (total > MAX_SEGMENTS) total = MAX_SEGMENTS;

        printf("[SMPP] Worker %d long ASCII msg: %d chars → %d parts ref=%u\n",
               w->id, len, total, ref);

        uint32_t last_seq = 0;
        for (int part = 0; part < total; part++) {
            int offset  = part * GSM_SEGMENT;
            int seg_len = len - offset;
            if (seg_len > GSM_SEGMENT) seg_len = GSM_SEGMENT;

            uint8_t seg[UDH_SIZE + GSM_SEGMENT];
            seg[0] = 0x05; seg[1] = 0x00; seg[2] = 0x03;
            seg[3] = ref;
            seg[4] = (uint8_t)total;
            seg[5] = (uint8_t)(part + 1);
            memcpy(seg + UDH_SIZE, msg + offset, seg_len);

            last_seq = smpp_send_pdu(w, from, to, 0x00, 0x40,
                                     seg, UDH_SIZE + seg_len, txid, msg);
        }
        return last_seq;

    } else {
        /* ---- UCS-2 path ---- */
        uint8_t ucs2[MAX_SEGMENTS * UCS2_SEGMENT * 2];
        int char_count = utf8_to_ucs2(msg, ucs2,
                                      MAX_SEGMENTS * UCS2_SEGMENT);

        if (char_count <= UCS2_SINGLE) {
            return smpp_send_pdu(w, from, to, 0x08, 0x00,
                                 ucs2, char_count * 2, txid, msg);
        }

        /* ---- multi-part UCS-2 ---- */
        uint8_t ref;
        pthread_mutex_lock(&ref_lock);
        ref = ref_counter++;
        pthread_mutex_unlock(&ref_lock);

        int total = (char_count + UCS2_SEGMENT - 1) / UCS2_SEGMENT;
        if (total > MAX_SEGMENTS) total = MAX_SEGMENTS;

        printf("[SMPP] Worker %d long UCS-2 msg: %d chars → %d parts ref=%u\n",
               w->id, char_count, total, ref);

        uint32_t last_seq = 0;
        for (int part = 0; part < total; part++) {
            int offset_chars = part * UCS2_SEGMENT;
            int seg_chars    = char_count - offset_chars;
            if (seg_chars > UCS2_SEGMENT) seg_chars = UCS2_SEGMENT;

            uint8_t seg[UDH_SIZE + UCS2_SEGMENT * 2];
            seg[0] = 0x05; seg[1] = 0x00; seg[2] = 0x03;
            seg[3] = ref;
            seg[4] = (uint8_t)total;
            seg[5] = (uint8_t)(part + 1);
            memcpy(seg + UDH_SIZE, ucs2 + offset_chars * 2, seg_chars * 2);

            last_seq = smpp_send_pdu(w, from, to, 0x08, 0x40,
                                     seg, UDH_SIZE + seg_chars * 2, txid, msg);
        }
        return last_seq;
    }
}

/* ------------------------------------------------ */

/* Advance pos past a NUL-terminated C-string, staying within max. */
static int skip_cstr(const uint8_t* buf, int pos, int max) {
    while (pos < max && buf[pos] != 0) pos++;
    return pos < max ? pos + 1 : max;
}

/* ------------------------------------------------ */

void* smpp_read_loop(void* arg) {
    SmppWorker* w = (SmppWorker*)arg;

    uint8_t header[16];
    uint8_t body[MAX_BODY];

    while (1) {
        if (read_full(w->socket, header, 16) <= 0) break;

        uint32_t len, cmd, status, seq;

        read_u32(header, 0, &len);
        read_u32(header, 4, &cmd);
        read_u32(header, 8, &status);
        read_u32(header, 12, &seq);

        if (len < 16 || len > MAX_BODY) {
            printf("[SMPP] invalid packet length %u\n", len);
            break;
        }

        if (read_full(w->socket, body, len - 16) <= 0) break;

        if (cmd == 0x80000009) {
            if (status == 0)
                printf("[SMPP] Worker %d bind successful\n", w->id);
        }

        else if (cmd == 0x80000004) {
            char message_id[64] = {0};

            if (status == 0) {
                /* body is the SMSC-assigned message_id string */
                size_t mid_len = strnlen((char*)body, len - 16);
                if (mid_len > sizeof(message_id) - 1)
                    mid_len = sizeof(message_id) - 1;
                memcpy(message_id, body, mid_len);
            }

            int idx = seq % MAX_SEQ;

            char txid[64]  = {0};
            char text[512] = {0};
            long sub_us;

            pthread_mutex_lock(&w->lock);

            snprintf(txid, sizeof(txid), "%s", w->seq_map[idx].txid);
            snprintf(text, sizeof(text), "%s", w->seq_map[idx].text);
            sub_us = w->seq_map[idx].submit_us;

            w->seq_map[idx].txid[0]   = 0;
            w->seq_map[idx].text[0]   = 0;
            w->seq_map[idx].submit_us = 0;

            pthread_mutex_unlock(&w->lock);

            wsem_post(&w->window);
            if (sub_us > 0)
                rtt_observe((now_us() - sub_us) / 1000);

            char ev_status[32];
            if (status == 0) {
                snprintf(ev_status, sizeof(ev_status), "SUBMITTED");
                atomic_fetch_add(&m_submit_ok, 1);
            } else {
                snprintf(ev_status, sizeof(ev_status), "ERROR:0x%08X", status);
                atomic_fetch_add(&m_submit_err, 1);
                printf("[SMPP] Worker %d submit_sm_resp error seq=%u "
                       "status=0x%08X txid=%s\n", w->id, seq, status, txid);
            }

            char json[512];

            snprintf(json, sizeof(json),
                     "{\"message_id\":\"%s\",\"transaction_id\":\"%s\","
                     "\"message\":\"%s\",\"status\":\"%s\"}",
                     message_id, txid, text, ev_status);

            publish_event(nc, config.nats_subject_submit, json);
        }

        else if (cmd == 0x00000005) {
            char message_id[64] = {0};
            char dlr_status[32] = {0};

            int body_len = (int)(len - 16);
            int pos = 0;

            /* skip service_type */
            pos = skip_cstr(body, pos, body_len);

            /* source addr: ton + npi + address */
            if (pos + 2 >= body_len) goto publish_dlr;
            pos += 2;
            pos = skip_cstr(body, pos, body_len);

            /* dest addr: ton + npi + address */
            if (pos + 2 >= body_len) goto publish_dlr;
            pos += 2;
            pos = skip_cstr(body, pos, body_len);

            /* fixed single-byte fields */
            if (pos + 7 >= body_len) goto publish_dlr;
            pos += 1; /* esm_class */
            pos += 1; /* protocol_id */
            pos += 1; /* priority */

            pos = skip_cstr(body, pos, body_len); /* schedule_delivery_time */
            pos = skip_cstr(body, pos, body_len); /* validity_period */

            if (pos + 4 >= body_len) goto publish_dlr;
            pos += 1; /* registered_delivery */
            pos += 1; /* replace_if_present */
            pos += 1; /* data_coding */
            pos += 1; /* sm_default_msg_id */

            if (pos >= body_len) goto publish_dlr;
            int sm_len = body[pos++];
            if (pos + sm_len > body_len) sm_len = body_len - pos;

            /* NUL-terminate short message for safe strstr */
            char sm_text[256] = {0};
            int copy_len = sm_len < 255 ? sm_len : 255;
            memcpy(sm_text, body + pos, copy_len);

            /* parse id: */
            char* id_p = strstr(sm_text, "id:");
            if (id_p) {
                id_p += 3;
                char* end = strchr(id_p, ' ');
                int l = end ? (int)(end - id_p) : (int)strlen(id_p);
                if (l > 63) l = 63;
                memcpy(message_id, id_p, l);
                message_id[l] = 0;
            }

            /* parse stat: — the actual delivery status */
            char* stat_p = strstr(sm_text, "stat:");
            if (stat_p) {
                stat_p += 5;
                char* end = strchr(stat_p, ' ');
                int sl = end ? (int)(end - stat_p) : (int)strlen(stat_p);
                if (sl > 31) sl = 31;
                memcpy(dlr_status, stat_p, sl);
                dlr_status[sl] = 0;
            }
            if (dlr_status[0] == 0)
                strncpy(dlr_status, "UNKNOWN", sizeof(dlr_status));

            /* parse err: — SMSC error code */
            char dlr_err[16] = {0};
            char* err_p = strstr(sm_text, "err:");
            if (err_p) {
                err_p += 4;
                char* end = strchr(err_p, ' ');
                int el = end ? (int)(end - err_p) : (int)strlen(err_p);
                if (el > 15) el = 15;
                memcpy(dlr_err, err_p, el);
                dlr_err[el] = 0;
            }
            if (dlr_err[0] == 0) strncpy(dlr_err, "000", sizeof(dlr_err));

            atomic_fetch_add(&m_dlr_total, 1);

publish_dlr:;
            char json[256];

            snprintf(json, sizeof(json),
                     "{\"message_id\":\"%s\",\"status\":\"%s\",\"error\":\"%s\"}",
                     message_id, dlr_status, dlr_err);

            publish_event(nc, config.nats_subject_delivery, json);
        }
    }

    printf("[SMPP] Worker %d socket closed\n", w->id);

    return NULL;
}

/* ------------------------------------------------ */

void* smpp_worker_loop(void* arg) {
    SmppWorker* w = (SmppWorker*)arg;
    int backoff = 1; /* seconds; doubles on each failure, caps at 60 */

    while (running) {
        printf("[SMPP] Worker %d connecting...\n", w->id);

        if (smpp_connect(w) != 0) {
            printf("[SMPP] Worker %d connect failed, retry in %ds\n",
                   w->id, backoff);
            sleep(backoff);
            backoff = backoff < 60 ? backoff * 2 : 60;
            continue;
        }

        backoff = 1; /* reset on successful connect */
        atomic_fetch_add(&m_sessions, 1);

        smpp_bind(w);

        printf("[SMPP] Worker %d entering read loop\n", w->id);

        smpp_read_loop(w);

        printf("[SMPP] Worker %d disconnected\n", w->id);
        atomic_fetch_sub(&m_sessions, 1);

        pthread_mutex_lock(&w->socket_lock);
        close(w->socket);
        w->socket = -1;
        pthread_mutex_unlock(&w->socket_lock);

        /* Unblock any sender threads waiting on the window. */
        wsem_reset(&w->window);

        if (!running) break;

        printf("[SMPP] Worker %d retry in %ds\n", w->id, backoff);
        sleep(backoff);
        backoff = backoff < 60 ? backoff * 2 : 60;
    }

    return NULL;
}

/* ------------------------------------------------ */
/* Prometheus metrics HTTP server                   */
/* ------------------------------------------------ */

static void metrics_write(int fd) {
    char body[4096];
    int  n = 0;

    static const char* le[RTT_BUCKET_N] =
        {"10","25","50","100","250","500","1000","2500","5000"};

    n += snprintf(body + n, sizeof(body) - n,
        "# HELP sms_submitted_total Total submit_sm PDUs sent\n"
        "# TYPE sms_submitted_total counter\n"
        "sms_submitted_total{status=\"success\"} %ld\n"
        "sms_submitted_total{status=\"failed\"} %ld\n"
        "# HELP sms_dlr_received_total Delivery receipts received\n"
        "# TYPE sms_dlr_received_total counter\n"
        "sms_dlr_received_total %ld\n"
        "# HELP smpp_sessions_connected Active SMPP sessions\n"
        "# TYPE smpp_sessions_connected gauge\n"
        "smpp_sessions_connected %ld\n"
        "# HELP sms_submit_rtt_ms submit_sm round-trip time in milliseconds\n"
        "# TYPE sms_submit_rtt_ms histogram\n",
        atomic_load(&m_submit_ok),
        atomic_load(&m_submit_err),
        atomic_load(&m_dlr_total),
        atomic_load(&m_sessions));

    for (int i = 0; i < RTT_BUCKET_N; i++)
        n += snprintf(body + n, sizeof(body) - n,
            "sms_submit_rtt_ms_bucket{le=\"%s\"} %ld\n",
            le[i], atomic_load(&rtt_bucket[i]));

    n += snprintf(body + n, sizeof(body) - n,
        "sms_submit_rtt_ms_bucket{le=\"+Inf\"} %ld\n"
        "sms_submit_rtt_ms_sum %ld\n"
        "sms_submit_rtt_ms_count %ld\n",
        atomic_load(&rtt_bucket[RTT_BUCKET_N]),
        atomic_load(&rtt_sum_ms),
        atomic_load(&rtt_count));

    char resp[5120];
    int  rlen = snprintf(resp, sizeof(resp),
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: text/plain; version=0.0.4\r\n"
        "Content-Length: %d\r\n"
        "Connection: close\r\n\r\n%s", n, body);

    write(fd, resp, rlen);
}

static void* metrics_server(void* arg) {
    int port = *(int*)arg;

    int srv = socket(AF_INET, SOCK_STREAM, 0);
    if (srv < 0) { perror("[Metrics] socket"); return NULL; }

    int opt = 1;
    setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr = {0};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(port);

    if (bind(srv, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("[Metrics] bind"); close(srv); return NULL;
    }

    listen(srv, 8);
    printf("[Metrics] listening on :%d\n", port);

    while (running) {
        int cli = accept(srv, NULL, NULL);
        if (cli < 0) continue;
        char req[256];
        read(cli, req, sizeof(req) - 1); /* drain request */
        metrics_write(cli);
        close(cli);
    }

    close(srv);
    return NULL;
}

/* ------------------------------------------------ */
/* submit_sm_resp timeout reaper                    */
/* ------------------------------------------------ */

/*
 * Runs every second.  Scans all workers' seq_maps for entries whose
 * submit_us is older than smpp_resp_timeout_ms.  For each one:
 *   - clears the slot
 *   - releases the window semaphore
 *   - publishes a TIMEOUT submit event so the logger records it
 *   - increments m_submit_err
 *
 * Uses per-worker lock → wsem_post ordering (same as the read loop),
 * so no deadlock is possible.
 */
static void* resp_reaper(void* arg) {
    (void)arg;

    while (running) {
        sleep(1);

        if (config.smpp_resp_timeout_ms <= 0) continue;

        long timeout_us = (long)config.smpp_resp_timeout_ms * 1000L;
        long now        = now_us();

        for (int wi = 0; wi < worker_count; wi++) {
            SmppWorker* w = &workers[wi];

            /* Collect expired entries under lock, process outside. */
            struct { char txid[64]; char text[512]; } expired[32];
            int ne = 0;

            pthread_mutex_lock(&w->lock);
            for (int i = 0; i < MAX_SEQ && ne < 32; i++) {
                if (w->seq_map[i].submit_us == 0) continue;
                if (now - w->seq_map[i].submit_us <= timeout_us) continue;

                snprintf(expired[ne].txid, 64,  "%s", w->seq_map[i].txid);
                snprintf(expired[ne].text, 512, "%s", w->seq_map[i].text);
                ne++;

                w->seq_map[i].txid[0]   = 0;
                w->seq_map[i].text[0]   = 0;
                w->seq_map[i].submit_us = 0;
            }
            pthread_mutex_unlock(&w->lock);

            for (int e = 0; e < ne; e++) {
                printf("[SMPP] Worker %d resp timeout txid=%s\n",
                       w->id, expired[e].txid);

                char json[512];
                snprintf(json, sizeof(json),
                         "{\"message_id\":\"\",\"transaction_id\":\"%s\","
                         "\"message\":\"%s\",\"status\":\"TIMEOUT\"}",
                         expired[e].txid, expired[e].text);

                publish_event(nc, config.nats_subject_submit, json);
                wsem_post(&w->window);
                atomic_fetch_add(&m_submit_err, 1);
            }
        }
    }

    return NULL;
}

/* ------------------------------------------------ */
int main() {
    if (load_config("config/gateway.conf") != 0) return 1;

    global_tps = config.tps;

    if (global_tps > 0) {
        send_interval_us = 1000000 / global_tps;
    }

    worker_count = config.smpp_workers;

    if (worker_count > MAX_WORKERS) worker_count = MAX_WORKERS;

    natsStatus s;

    s = natsConnection_ConnectTo(&nc, config.nats_url);

    if (s != NATS_OK) {
        printf("[NATS] connection failed\n");
        return 1;
    }

    printf("[NATS] connected\n");

    signal(SIGINT,  on_signal);
    signal(SIGTERM, on_signal);

    for (int i = 0; i < worker_count; i++) {
        workers[i].id = i;
        workers[i].seq = 0;
        workers[i].socket = -1;

        pthread_mutex_init(&workers[i].lock, NULL);
        pthread_mutex_init(&workers[i].socket_lock, NULL);
        wsem_init(&workers[i].window, config.smpp_window_size);

        pthread_create(&workers[i].thread, NULL, smpp_worker_loop, &workers[i]);
    }

    if (config.metrics_port > 0) {
        pthread_t metrics_tid;
        pthread_create(&metrics_tid, NULL, metrics_server, &config.metrics_port);
        pthread_detach(metrics_tid);
    }

    if (config.smpp_resp_timeout_ms > 0) {
        pthread_t reaper_tid;
        pthread_create(&reaper_tid, NULL, resp_reaper, NULL);
        pthread_detach(reaper_tid);
        printf("[Consumer] resp timeout reaper started (%dms)\n",
               config.smpp_resp_timeout_ms);
    }

    jsCtx* js = NULL;
    jsOptions opts;

    jsOptions_Init(&opts);

    if (natsConnection_JetStream(&js, nc, &opts) != NATS_OK) {
        printf("[NATS] JetStream init failed\n");
        return 1;
    }

    jsSubOptions subOpts;
    jsSubOptions_Init(&subOpts);

    subOpts.Stream = config.nats_stream_name;

    jsErrCode jerr;

    natsSubscription* sub = NULL;

    natsStatus ps = js_PullSubscribe(&sub, js, config.nats_subject_outgoing,
                                     config.nats_consumer_smpp, NULL, &subOpts,
                                     &jerr);

    if (ps != NATS_OK || sub == NULL) {
        printf("[NATS] PullSubscribe failed: %s (js err %d)\n",
               natsStatus_GetText(ps), jerr);
        return 1;
    }

    printf("[NATS] consumer ready\n");

    while (running) {
        natsMsgList msgs;

        natsStatus fs =
            natsSubscription_Fetch(&msgs, sub, BATCH_SIZE, WAIT_TIMEOUT, &jerr);

        if (fs != NATS_OK) continue;

        for (int i = 0; i < msgs.Count; i++) {
            const char* payload = natsMsg_GetData(msgs.Msgs[i]);

            char txid[64]     = {0};
            char sender[32]   = {0};
            char dest[32]     = {0};
            char text[512]    = {0};
            char msg_type[32] = {0};

            extract_json(payload, "transaction_id", txid,     sizeof(txid));
            extract_json(payload, "sender",         sender,   sizeof(sender));
            extract_json(payload, "destination",    dest,     sizeof(dest));
            extract_json(payload, "message",        text,     sizeof(text));
            extract_json(payload, "message_type",   msg_type, sizeof(msg_type));

            /* Drop marketing messages outside the configured send window. */
            if (strcmp(msg_type, "marketing") == 0 &&
                !within_send_window(config.send_window_morning,
                                    config.send_window_evening)) {
                printf("[Consumer] dropping marketing msg txid=%s (outside window)\n",
                       txid);
                natsMsg_Ack(msgs.Msgs[i], NULL);
                continue;
            }

            SmppWorker* w = &workers[rr++ % worker_count];

            global_tps_wait();
            uint32_t seq = smpp_submit(w, sender, dest, text, txid);

            /*
             * seq == 0 means the write to the socket failed (worker
             * disconnected).  When retry is enabled, NAK the message so
             * NATS redelivers it once the worker reconnects.
             */
            if (seq == 0 && config.smpp_retry_on_fail) {
                natsMsg_Nak(msgs.Msgs[i], NULL);
            } else {
                natsMsg_Ack(msgs.Msgs[i], NULL);
            }
        }

        natsMsgList_Destroy(&msgs);
    }

    /* ---- clean shutdown ---- */
    printf("[Consumer] shutting down...\n");

    for (int i = 0; i < worker_count; i++) {
        pthread_mutex_lock(&workers[i].socket_lock);
        if (workers[i].socket >= 0) {
            close(workers[i].socket);
            workers[i].socket = -1;
        }
        pthread_mutex_unlock(&workers[i].socket_lock);
        wsem_reset(&workers[i].window); /* unblock any thread waiting on window */
    }

    for (int i = 0; i < worker_count; i++)
        pthread_join(workers[i].thread, NULL);

    natsSubscription_Destroy(sub);
    jsCtx_Destroy(js);
    natsConnection_Drain(nc);
    natsConnection_Destroy(nc);

    printf("[Consumer] shutdown complete\n");
    return 0;
}
