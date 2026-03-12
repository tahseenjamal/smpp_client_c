#include <hiredis/hiredis.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

redisContext* redis_connect() {
    redisContext* c = redisConnect("127.0.0.1", 6379);

    if (c == NULL || c->err) {
        printf("Redis connection error\n");
        return NULL;
    }

    return c;
}

void store_mapping(redisContext* c, const char* msg_id, const char* tx) {
    redisReply* reply;

    reply = redisCommand(c, "SETEX sms:%s 86400 %s", msg_id, tx);

    if (reply) freeReplyObject(reply);
}

char* lookup_transaction(redisContext* c, const char* msg_id) {
    redisReply* reply;

    reply = redisCommand(c, "GET sms:%s", msg_id);

    char* result = NULL;

    if (reply && reply->type == REDIS_REPLY_STRING) result = strdup(reply->str);

    if (reply) freeReplyObject(reply);

    return result;
}
