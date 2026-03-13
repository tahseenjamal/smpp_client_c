#include <arpa/inet.h>
#include <nats/nats.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
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

/* ------------------------------------------------ */

typedef struct {
    char txid[64];
    char text[512];
} SeqMap;

typedef struct {
    int id;
    int socket;
    pthread_t thread;

    pthread_mutex_t lock;

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

    w->socket = socket(AF_INET, SOCK_STREAM, 0);

    if (w->socket < 0) {
        perror("socket");
        return -1;
    }

    addr.sin_family = AF_INET;
    addr.sin_port = htons(config.smpp_port);
    addr.sin_addr.s_addr = inet_addr(config.smpp_host);

    if (connect(w->socket, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("SMPP connect");
        return -1;
    }

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

    write(w->socket, pdu, pos);

    printf("[SMPP] Worker %d bind_transceiver seq=%u\n", w->id, seq);
}

/* ------------------------------------------------ */

uint32_t smpp_submit(SmppWorker* w, const char* from, const char* to,
                     const char* msg, const char* txid) {
    uint8_t pdu[1024];
    int pos = 16;

    pdu[pos++] = 0;

    pdu[pos++] = 1;
    pdu[pos++] = 1;
    write_cstring(pdu, &pos, from);

    pdu[pos++] = 1;
    pdu[pos++] = 1;
    write_cstring(pdu, &pos, to);

    pdu[pos++] = 0;
    pdu[pos++] = 0;
    pdu[pos++] = 0;

    pdu[pos++] = 0;
    pdu[pos++] = 0;

    pdu[pos++] = 1;

    pdu[pos++] = 0;
    pdu[pos++] = 0;
    pdu[pos++] = 0;

    int len = strlen(msg);
    if (len > 254) len = 254;

    pdu[pos++] = len;

    memcpy(pdu + pos, msg, len);
    pos += len;

    uint32_t seq = next_seq(w);

    write_u32(pdu, 0, pos);
    write_u32(pdu, 4, 0x00000004);
    write_u32(pdu, 8, 0);
    write_u32(pdu, 12, seq);

    int idx = seq % MAX_SEQ;

    pthread_mutex_lock(&w->lock);

    snprintf(w->seq_map[idx].txid, sizeof(w->seq_map[idx].txid), "%s", txid);
    snprintf(w->seq_map[idx].text, sizeof(w->seq_map[idx].text), "%s", msg);

    pthread_mutex_unlock(&w->lock);

    write(w->socket, pdu, pos);

    return seq;
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

            size_t mid_len = strnlen((char*)body, len - 16);
            if (mid_len > sizeof(message_id) - 1)
                mid_len = sizeof(message_id) - 1;

            memcpy(message_id, body, mid_len);

            int idx = seq % MAX_SEQ;

            char txid[64];
            char text[512];

            pthread_mutex_lock(&w->lock);

            snprintf(txid, sizeof(txid), "%s", w->seq_map[idx].txid);
            snprintf(text, sizeof(text), "%s", w->seq_map[idx].text);

            /* clear mapping */
            w->seq_map[idx].txid[0] = 0;
            w->seq_map[idx].text[0] = 0;

            pthread_mutex_unlock(&w->lock);

            char json[512];

            snprintf(json, sizeof(json),
                     "{\"message_id\":\"%s\",\"transaction_id\":\"%s\","
                     "\"message\":\"%s\"}",
                     message_id, txid, text);

            publish_event(nc, config.nats_subject_submit, json);
        }

        else if (cmd == 0x00000005) {
            char message_id[64] = {0};

            int pos = 0;

            /* skip service_type */
            pos += strlen((char*)body + pos) + 1;

            /* source addr */
            pos += 2;
            pos += strlen((char*)body + pos) + 1;

            /* dest addr */
            pos += 2;
            pos += strlen((char*)body + pos) + 1;

            /* skip fixed fields */
            pos += 1; /* esm_class */
            pos += 1; /* protocol_id */
            pos += 1; /* priority */

            pos += strlen((char*)body + pos) + 1; /* schedule_delivery */
            pos += strlen((char*)body + pos) + 1; /* validity_period */

            pos += 1; /* registered_delivery */
            pos += 1; /* replace_if_present */
            pos += 1; /* data_coding */
            pos += 1; /* sm_default_msg_id */

            int sm_len = body[pos++];
            char* text = (char*)body + pos;

            /* parse id: from receipt text */

            char* id = strstr(text, "id:");

            if (id) {
                id += 3;

                char* end = strchr(id, ' ');

                if (end) {
                    int l = end - id;
                    if (l > 63) l = 63;

                    memcpy(message_id, id, l);
                    message_id[l] = 0;
                }
            }

            char json[256];

            snprintf(json, sizeof(json),
                     "{\"message_id\":\"%s\",\"status\":\"DELIVRD\"}",
                     message_id);

            publish_event(nc, config.nats_subject_delivery, json);
        }
    }

    printf("[SMPP] Worker %d socket closed\n", w->id);

    return NULL;
}

/* ------------------------------------------------ */

void* smpp_worker_loop(void* arg) {
    SmppWorker* w = (SmppWorker*)arg;

    while (1) {
        printf("[SMPP] Worker %d connecting...\n", w->id);

        if (smpp_connect(w) != 0) {
            sleep(3);
            continue;
        }

        smpp_bind(w);

        printf("[SMPP] Worker %d entering read loop\n", w->id);

        smpp_read_loop(w);  // ← your existing read loop

        printf("[SMPP] Worker %d disconnected\n", w->id);

        close(w->socket);

        sleep(3);
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

    for (int i = 0; i < worker_count; i++) {
        workers[i].id = i;
        workers[i].seq = 0;

        pthread_mutex_init(&workers[i].lock, NULL);

        if (smpp_connect(&workers[i]) != 0) return 1;

        smpp_bind(&workers[i]);

        pthread_create(&workers[i].thread, NULL, smpp_worker_loop, &workers[i]);
    }

    jsCtx* js = NULL;
    jsOptions opts;

    jsOptions_Init(&opts);

    natsConnection_JetStream(&js, nc, &opts);

    jsSubOptions subOpts;
    jsSubOptions_Init(&subOpts);

    subOpts.Stream = config.nats_stream_name;

    jsErrCode jerr;

    natsSubscription* sub = NULL;

    js_PullSubscribe(&sub, js, config.nats_subject_outgoing,
                     config.nats_consumer_smpp, NULL, &subOpts, &jerr);

    printf("[NATS] consumer ready\n");

    while (1) {
        natsMsgList msgs;

        natsStatus fs =
            natsSubscription_Fetch(&msgs, sub, BATCH_SIZE, WAIT_TIMEOUT, &jerr);

        if (fs != NATS_OK) continue;

        for (int i = 0; i < msgs.Count; i++) {
            const char* payload = natsMsg_GetData(msgs.Msgs[i]);

            char txid[64] = {0};
            char sender[32] = {0};
            char dest[32] = {0};
            char text[512] = {0};

            extract_json(payload, "transaction_id", txid, sizeof(txid));
            extract_json(payload, "sender", sender, sizeof(sender));
            extract_json(payload, "destination", dest, sizeof(dest));
            extract_json(payload, "message", text, sizeof(text));

            SmppWorker* w = &workers[rr++ % worker_count];

            global_tps_wait();
            smpp_submit(w, sender, dest, text, txid);

            natsMsg_Ack(msgs.Msgs[i], NULL);
            // ✅ Remove natsMsg_Destroy(msgs.Msgs[i]) — natsMsgList_Destroy
            // handles this
        }

        natsMsgList_Destroy(&msgs);  // ✅ Single cleanup point
    }

    return 0;
}
