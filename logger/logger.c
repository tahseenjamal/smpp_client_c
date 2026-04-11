#include <curl/curl.h>
#include <hiredis/hiredis.h>
#include <nats/nats.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "../common/config.h"

#define BATCH_SIZE 100
#define WAIT_TIMEOUT 1000
#define MAX_MSG 2048

FILE* submit_log;
FILE* delivery_log;
redisContext* redis;

static void ensure_redis() {
    if (redis && !redis->err) return;
    if (redis) redisFree(redis);
    redis = redisConnect(config.redis_host, config.redis_port);
    if (!redis || redis->err) {
        printf("[Redis] reconnect failed\n");
        redis = NULL;
    } else {
        printf("[Redis] reconnected\n");
    }
}

/* ------------------------------------------------ */

void now(char* buf, int size) {
    time_t t = time(NULL);
    struct tm* tm = gmtime(&t);
    strftime(buf, size, "%Y-%m-%dT%H:%M:%SZ", tm);
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

void send_dlr_callback(const char* txid, const char* message_id,
                       const char* status) {
    if (strlen(config.dlr_callback) == 0) return;

    CURL* curl = curl_easy_init();
    if (!curl) return;

    char json[512];

    snprintf(
        json, sizeof(json),
        "{\"transaction_id\":\"%s\",\"message_id\":\"%s\",\"status\":\"%s\"}",
        txid, message_id, status);

    struct curl_slist* headers = NULL;
    headers = curl_slist_append(headers, "Content-Type: application/json");

    curl_easy_setopt(curl, CURLOPT_URL, config.dlr_callback);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, json);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT_MS, 2000L);

    curl_easy_perform(curl);

    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
}

/* ------------------------------------------------ */

void process_submit(const char* payload) {
    char message_id[64] = {0};
    char txid[64] = {0};
    char message[512] = {0};

    extract_json(payload, "message_id", message_id, sizeof(message_id));
    extract_json(payload, "transaction_id", txid, sizeof(txid));
    extract_json(payload, "message", message, sizeof(message));

    char ts[64];
    now(ts, sizeof(ts));

    ensure_redis();
    if (redis) {
        redisReply* r =
            redisCommand(redis, "SETEX sms:%s 86400 %s", message_id, txid);
        if (r) freeReplyObject(r);
    }

    fprintf(submit_log, "%s|%s|%s|SUBMITTED|%s\n", ts, message_id, txid,
            message);
}

/* ------------------------------------------------ */

void process_delivery(const char* payload) {
    char message_id[64] = {0};
    char status[32] = {0};

    extract_json(payload, "message_id", message_id, sizeof(message_id));
    extract_json(payload, "status", status, sizeof(status));

    char ts[64];
    now(ts, sizeof(ts));

    ensure_redis();

    redisReply* r = NULL;
    if (redis) r = redisCommand(redis, "GET sms:%s", message_id);

    char* txid = NULL;
    if (r && r->type == REDIS_REPLY_STRING) txid = r->str;

    fprintf(delivery_log, "%s|%s|%s|%s\n", ts, message_id,
            txid ? txid : "UNKNOWN", status);

    if (txid) send_dlr_callback(txid, message_id, status);

    if (r) freeReplyObject(r);
}

/* ------------------------------------------------ */

int main() {
    printf("Starting logger\n");

    if (load_config("config/gateway.conf") != 0) return 1;

    curl_global_init(CURL_GLOBAL_DEFAULT);

    mkdir(config.log_dir, 0755);

    char submit_path[256];
    char delivery_path[256];

    snprintf(submit_path, sizeof(submit_path), "%s/submit.log", config.log_dir);

    snprintf(delivery_path, sizeof(delivery_path), "%s/delivery.log",
             config.log_dir);

    submit_log = fopen(submit_path, "a");
    delivery_log = fopen(delivery_path, "a");

    if (!submit_log || !delivery_log) {
        perror("log open failed");
        return 1;
    }

    redis = redisConnect(config.redis_host, config.redis_port);

    if (!redis || redis->err) {
        printf("Redis connection failed\n");
        return 1;
    }

    printf("Connected to Redis\n");

    /* ---------------- NATS ---------------- */

    natsConnection* nc = NULL;
    jsCtx* js = NULL;

    if (natsConnection_ConnectTo(&nc, config.nats_url) != NATS_OK) {
        printf("NATS connect failed\n");
        return 1;
    }

    jsOptions jsOpts;
    jsOptions_Init(&jsOpts);

    if (natsConnection_JetStream(&js, nc, &jsOpts) != NATS_OK) {
        printf("JetStream init failed\n");
        return 1;
    }

    printf("JetStream ready\n");

    /* ---------------- JetStream Pull ---------------- */

    jsSubOptions subOpts;
    jsSubOptions_Init(&subOpts);

    /* READ STREAM FROM CONFIG */
    subOpts.Stream = config.nats_stream_name;

    jsErrCode jerr;

    natsSubscription* sub_submit = NULL;
    natsSubscription* sub_delivery = NULL;

    natsStatus ps;

    ps = js_PullSubscribe(&sub_submit, js, config.nats_subject_submit,
                          "logger_submit", NULL, &subOpts, &jerr);
    if (ps != NATS_OK || sub_submit == NULL) {
        printf("PullSubscribe submit failed: %s (js err %d)\n",
               natsStatus_GetText(ps), jerr);
        return 1;
    }

    ps = js_PullSubscribe(&sub_delivery, js, config.nats_subject_delivery,
                          "logger_delivery", NULL, &subOpts, &jerr);
    if (ps != NATS_OK || sub_delivery == NULL) {
        printf("PullSubscribe delivery failed: %s (js err %d)\n",
               natsStatus_GetText(ps), jerr);
        return 1;
    }

    printf("Logger pull subscribers running\n");

    /* ---------------- Main Loop ---------------- */

    while (1) {
        natsMsgList msgs;
        memset(&msgs, 0, sizeof(msgs));

        natsStatus s = natsSubscription_Fetch(&msgs, sub_submit, BATCH_SIZE,
                                              WAIT_TIMEOUT, &jerr);

        if (s == NATS_OK) {
            for (int i = 0; i < msgs.Count; i++) {
                const char* data = natsMsg_GetData(msgs.Msgs[i]);

                char safe[MAX_MSG];

                int len = natsMsg_GetDataLength(msgs.Msgs[i]);
                if (len >= MAX_MSG) len = MAX_MSG - 1;

                memcpy(safe, data, len);
                safe[len] = 0;

                process_submit(safe);

                natsMsg_Ack(msgs.Msgs[i], NULL);
            }

            natsMsgList_Destroy(&msgs);
        }

        memset(&msgs, 0, sizeof(msgs));

        s = natsSubscription_Fetch(&msgs, sub_delivery, BATCH_SIZE,
                                   WAIT_TIMEOUT, &jerr);

        if (s == NATS_OK) {
            for (int i = 0; i < msgs.Count; i++) {
                const char* data = natsMsg_GetData(msgs.Msgs[i]);

                char safe[MAX_MSG];

                int len = natsMsg_GetDataLength(msgs.Msgs[i]);
                if (len >= MAX_MSG) len = MAX_MSG - 1;

                memcpy(safe, data, len);
                safe[len] = 0;

                process_delivery(safe);

                natsMsg_Ack(msgs.Msgs[i], NULL);
            }

            natsMsgList_Destroy(&msgs);
        }

        fflush(submit_log);
        fflush(delivery_log);
    }

    return 0;
}
