#ifndef CONFIG_H
#define CONFIG_H

typedef struct {
    int producer_port;
    char producer_route[64];

    char nats_url[1024];
    char nats_stream_name[256];
    char nats_subject_outgoing[256];
    char nats_subject_submit[256];
    char nats_subject_delivery[256];
    char nats_consumer_smpp[256];

    char redis_host[64];
    int redis_port;

    char smpp_host[1024];
    int smpp_port;

    char smpp_system_id[256];
    char smpp_password[256];
    char smpp_system_type[256];

    int smpp_workers;
    int smpp_window_size;

    int tps;

    char dlr_callback[1024];

    char log_dir[256];

    char send_window_morning[16]; /* "HH:MM" — marketing send window start */
    char send_window_evening[16]; /* "HH:MM" — marketing send window end   */

    int metrics_port;             /* Prometheus /metrics HTTP port          */

} GatewayConfig;

extern GatewayConfig config;

int load_config(const char* path);

#endif
