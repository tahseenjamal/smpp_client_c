#include <hiredis/hiredis.h>
#include <nats/nats.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define MAX_BATCH 200

FILE* submit_log;
FILE* delivery_log;

char submit_buffer[MAX_BATCH][1024];
char delivery_buffer[MAX_BATCH][1024];

int submit_count = 0;
int delivery_count = 0;

redisContext* redis;

/* ------------------------------------------------ */
/* timestamp helper */
/* ------------------------------------------------ */

void now(char* buf, int size) {
    time_t t = time(NULL);
    struct tm* tm = gmtime(&t);
    strftime(buf, size, "%Y-%m-%dT%H:%M:%SZ", tm);
}

/* ------------------------------------------------ */
/* flush logs */
/* ------------------------------------------------ */

void flush_submit() {
    if (submit_count == 0) return;

    for (int i = 0; i < submit_count; i++)
        fprintf(submit_log, "%s", submit_buffer[i]);

    fflush(submit_log);
    submit_count = 0;
}

void flush_delivery() {
    if (delivery_count == 0) return;

    for (int i = 0; i < delivery_count; i++)
        fprintf(delivery_log, "%s", delivery_buffer[i]);

    fflush(delivery_log);
    delivery_count = 0;
}

/* ------------------------------------------------ */
/* redis helpers */
/* ------------------------------------------------ */

void store_mapping(const char* msgid, const char* txid) {
    redisReply* reply =
        redisCommand(redis, "SETEX sms:%s 86400 %s", msgid, txid);
    if (reply) freeReplyObject(reply);
}

char* lookup_mapping(const char* msgid) {
    redisReply* reply = redisCommand(redis, "GET sms:%s", msgid);

    char* res = NULL;

    if (reply && reply->type == REDIS_REPLY_STRING) res = strdup(reply->str);

    if (reply) freeReplyObject(reply);

    return res;
}

/* ------------------------------------------------ */
/* submit handler */
/* ------------------------------------------------ */

void on_submit(natsConnection* nc, natsSubscription* sub, natsMsg* msg,
               void* c) {
    char message_id[64] = {0};
    char txid[64] = {0};
    char text[512] = {0};

    sscanf(natsMsg_GetData(msg),
           "{\"message_id\":\"%63[^\"]\",\"transaction_id\":\"%63[^\"]\","
           "\"message\":\"%511[^\"]\"}",
           message_id, txid, text);

    char ts[64];
    now(ts, sizeof(ts));

    /* store mapping */

    redisReply* r =
        redisCommand(redis, "SETEX sms:%s 86400 %s", message_id, txid);
    if (r) freeReplyObject(r);

    /* write log */

    fprintf(submit_log, "%s|%s|%s|SUBMITTED|%s\n", ts, message_id, txid, text);
    fflush(submit_log);

    /*printf("SUBMIT %s %s\n", message_id, txid);*/

    natsMsg_Destroy(msg);
}

/* ------------------------------------------------ */
/* delivery handler */
/* ------------------------------------------------ */

void on_delivery(natsConnection* nc, natsSubscription* sub, natsMsg* msg,
                 void* c) {
    char message_id[64] = {0};
    char status[32] = {0};

    sscanf(natsMsg_GetData(msg),
           "{\"message_id\":\"%63[^\"]\",\"status\":\"%31[^\"]\"}", message_id,
           status);

    char* txid = NULL;

    redisReply* r = redisCommand(redis, "GET sms:%s", message_id);

    if (r && r->type == REDIS_REPLY_STRING) txid = r->str;

    char ts[64];
    now(ts, sizeof(ts));

    fprintf(delivery_log, "%s|%s|%s|%s\n", ts, message_id,
            txid ? txid : "UNKNOWN", status);

    fflush(delivery_log);

    if (r) freeReplyObject(r);

    /*printf("DLR %s %s\n", message_id, status);*/

    natsMsg_Destroy(msg);
}

/* ------------------------------------------------ */
/* main */
/* ------------------------------------------------ */

int main() {
    printf("Starting logger\n");

    mkdir("logs", 0755);

    submit_log = fopen("logs/submit.log", "a");
    delivery_log = fopen("logs/delivery.log", "a");

    if (!submit_log || !delivery_log) {
        perror("log open failed");
        exit(1);
    }

    /* Redis */

    redis = redisConnect("127.0.0.1", 6379);

    if (!redis || redis->err) {
        printf("Redis connection failed\n");
        exit(1);
    }

    printf("Connected to Redis\n");

    /* NATS */

    natsConnection* nc;
    natsSubscription* sub_submit;
    natsSubscription* sub_delivery;

    natsStatus s;

    s = natsConnection_ConnectTo(&nc, "nats://localhost:4222");

    if (s != NATS_OK) {
        printf("NATS connection failed: %s\n", natsStatus_GetText(s));
        exit(1);
    }

    printf("Connected to NATS\n");

    /* subscribe */

    s = natsConnection_Subscribe(&sub_submit, nc, "sms.submit", on_submit,
                                 NULL);

    if (s != NATS_OK) {
        printf("Subscribe submit failed\n");
        exit(1);
    }

    s = natsConnection_Subscribe(&sub_delivery, nc, "sms.delivery", on_delivery,
                                 NULL);

    if (s != NATS_OK) {
        printf("Subscribe delivery failed\n");
        exit(1);
    }

    printf("Logger running\n");

    while (1) {
        /* periodic flush */

        flush_submit();
        flush_delivery();

        usleep(200000);
    }

    return 0;
}
