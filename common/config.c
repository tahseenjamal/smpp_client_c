#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

GatewayConfig config;

/* trim whitespace */

static void trim(char* str) {
    char* start = str;
    char* end;

    while (*start == ' ' || *start == '\t' || *start == '\n') start++;

    if (start != str) memmove(str, start, strlen(start) + 1);

    end = str + strlen(str) - 1;

    while (end > str &&
           (*end == ' ' || *end == '\t' || *end == '\n' || *end == '\r')) {
        *end = '\0';
        end--;
    }
}

/* parse key=value */

static void set_value(const char* key, const char* value) {
    if (strcmp(key, "nats_url") == 0)
        strncpy(config.nats_url, value, sizeof(config.nats_url));

    else if (strcmp(key, "redis_host") == 0)
        strncpy(config.redis_host, value, sizeof(config.redis_host));

    else if (strcmp(key, "redis_port") == 0)
        config.redis_port = atoi(value);

    else if (strcmp(key, "smpp_host") == 0)
        strncpy(config.smpp_host, value, sizeof(config.smpp_host));

    else if (strcmp(key, "smpp_port") == 0)
        config.smpp_port = atoi(value);

    else if (strcmp(key, "smpp_system_id") == 0)
        strncpy(config.smpp_system_id, value, sizeof(config.smpp_system_id));

    else if (strcmp(key, "smpp_password") == 0)
        strncpy(config.smpp_password, value, sizeof(config.smpp_password));

    else if (strcmp(key, "smpp_workers") == 0)
        config.smpp_workers = atoi(value);

    else if (strcmp(key, "tps") == 0)
        config.tps = atoi(value);

    else if (strcmp(key, "dlr_callback") == 0)
        strncpy(config.dlr_callback, value, sizeof(config.dlr_callback));

    else if (strcmp(key, "producer_port") == 0)
        config.producer_port = atoi(value);

    else if (strcmp(key, "producer_route") == 0)
        strncpy(config.producer_route, value, sizeof(config.producer_route));

    else if (strcmp(key, "nats_subject_outgoing") == 0)
        strncpy(config.nats_subject_outgoing, value,
                sizeof(config.nats_subject_outgoing));

    else if (strcmp(key, "log_dir") == 0)
        strncpy(config.log_dir, value, sizeof(config.log_dir));

    else if (strcmp(key, "smpp_system_type") == 0)
        strncpy(config.smpp_system_type, value,
                sizeof(config.smpp_system_type));

    else if (strcmp(key, "smpp_window_size") == 0)
        config.smpp_window_size = atoi(value);
    else if (strcmp(key, "nats_subject_submit") == 0)
        strncpy(config.nats_subject_submit, value,
                sizeof(config.nats_subject_submit));

    else if (strcmp(key, "nats_subject_delivery") == 0)
        strncpy(config.nats_subject_delivery, value,
                sizeof(config.nats_subject_delivery));

    else if (strcmp(key, "nats_consumer_smpp") == 0)
        strncpy(config.nats_consumer_smpp, value,
                sizeof(config.nats_consumer_smpp));

    else if (strcmp(key, "nats_stream_name") == 0)
        strncpy(config.nats_stream_name, value,
                sizeof(config.nats_stream_name));
}

/* load config file */

int load_config(const char* path) {
    FILE* f = fopen(path, "r");

    if (!f) {
        printf("Failed to open config file: %s\n", path);
        return -1;
    }

    char line[512];

    while (fgets(line, sizeof(line), f)) {
        if (line[0] == '#' || strlen(line) < 3) continue;

        char* eq = strchr(line, '=');

        if (!eq) continue;

        *eq = 0;

        char* key = line;
        char* value = eq + 1;

        trim(key);
        trim(value);

        set_value(key, value);
    }

    fclose(f);

    printf("Config loaded\n");

    printf("NATS: %s\n", config.nats_url);
    printf("Redis: %s:%d\n", config.redis_host, config.redis_port);
    printf("SMPP: %s:%d\n", config.smpp_host, config.smpp_port);
    printf("Workers: %d\n", config.smpp_workers);
    printf("TPS: %d\n", config.tps);

    return 0;
}
