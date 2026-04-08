#include <microhttpd.h>
#include <nats/nats.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "../common/config.h"

static volatile sig_atomic_t running = 1;
static void on_signal(int sig) { (void)sig; running = 0; }

#define MAX_BODY 4096

natsConnection* nc;
jsCtx* js;

typedef struct {
    char body[MAX_BODY];
    size_t size;
    int overflow;
} RequestContext;

/* ------------------------------------------------ */
/* Ensure JetStream stream exists                   */
/* ------------------------------------------------ */

static int ensure_stream() {
    jsStreamInfo* si = NULL;
    jsErrCode jerr = 0;

    /* try lookup first */

    natsStatus s =
        js_GetStreamInfo(&si, js, config.nats_stream_name, NULL, &jerr);

    if (s == NATS_OK) {
        printf("[JetStream] Stream already exists: %s\n",
               config.nats_stream_name);
        jsStreamInfo_Destroy(si);
        return 0;
    }

    printf("[JetStream] Creating stream: %s\n", config.nats_stream_name);

    jsStreamConfig sc;
    jsStreamConfig_Init(&sc);

    sc.Name = config.nats_stream_name;

    const char* subjects[1];
    subjects[0] = config.nats_subject_outgoing;

    sc.Subjects = subjects;
    sc.SubjectsLen = 1;

    sc.Storage = 1; /* file storage */
    sc.MaxMsgs = -1;
    sc.MaxBytes = -1;
    sc.MaxAge = 0;

    s = js_AddStream(&si, js, &sc, NULL, &jerr);

    if (s != NATS_OK) {
        printf("[JetStream] Stream creation failed\n");
        printf("NATS status: %s\n", natsStatus_GetText(s));
        printf("JS error code: %d\n", jerr);
        return -1;
    }

    printf("[JetStream] Stream created successfully\n");

    if (si) jsStreamInfo_Destroy(si);

    return 0;
}

/* ------------------------------------------------ */
/* HTTP handler                                     */
/* ------------------------------------------------ */

static enum MHD_Result handler(void* cls, struct MHD_Connection* connection,
                               const char* url, const char* method,
                               const char* version, const char* upload_data,
                               size_t* upload_data_size, void** con_cls) {
    RequestContext* ctx = *con_cls;

    if (strcmp(method, "POST") != 0) return MHD_NO;

    if (strcmp(url, config.producer_route) != 0) return MHD_NO;

    /* first call */

    if (ctx == NULL) {
        ctx = calloc(1, sizeof(RequestContext));
        *con_cls = ctx;
        return MHD_YES;
    }

    /* receive POST body */

    if (*upload_data_size != 0) {
        if (ctx->size + *upload_data_size < MAX_BODY) {
            memcpy(ctx->body + ctx->size, upload_data, *upload_data_size);
            ctx->size += *upload_data_size;
        } else {
            ctx->overflow = 1;
        }
        *upload_data_size = 0;
        return MHD_YES;
    }

    if (ctx->overflow) {
        free(ctx);
        *con_cls = NULL;
        const char* msg = "payload too large";
        struct MHD_Response* res = MHD_create_response_from_buffer(
            strlen(msg), (void*)msg, MHD_RESPMEM_PERSISTENT);
        enum MHD_Result ret = MHD_queue_response(connection, 413, res);
        MHD_destroy_response(res);
        return ret;
    }

    /* ------------------------------------------------ */
    /* publish to JetStream                             */
    /* ------------------------------------------------ */

    jsPubAck* pa = NULL;
    jsErrCode jerr;

    natsStatus s = js_Publish(&pa, js, config.nats_subject_outgoing, ctx->body,
                              ctx->size, NULL, &jerr);

    if (s != NATS_OK) {
        printf("[JetStream] publish failed: %s\n", natsStatus_GetText(s));
    }

    if (pa != NULL) jsPubAck_Destroy(pa);

    free(ctx);
    *con_cls = NULL;

    const char* response = "queued";

    struct MHD_Response* res = MHD_create_response_from_buffer(
        strlen(response), (void*)response, MHD_RESPMEM_PERSISTENT);

    enum MHD_Result ret = MHD_queue_response(connection, MHD_HTTP_OK, res);

    MHD_destroy_response(res);

    return ret;
}

/* ------------------------------------------------ */
/* main                                             */
/* ------------------------------------------------ */

int main() {
    if (load_config("config/gateway.conf") != 0) {
        printf("Config load failed\n");
        return 1;
    }

    natsStatus s;

    /* connect to NATS */

    s = natsConnection_ConnectTo(&nc, config.nats_url);

    if (s != NATS_OK) {
        printf("NATS connect failed: %s\n", natsStatus_GetText(s));
        exit(1);
    }

    printf("[NATS] connected %s\n", config.nats_url);

    /* create JetStream context */

    jsOptions opts;
    jsOptions_Init(&opts);

    s = natsConnection_JetStream(&js, nc, &opts);

    if (s != NATS_OK) {
        printf("JetStream init failed: %s\n", natsStatus_GetText(s));
        exit(1);
    }

    printf("[JetStream] ready\n");

    /* auto create stream */

    if (ensure_stream() != 0) {
        printf("Stream initialization failed\n");
        exit(1);
    }

    /* start HTTP server */

    struct MHD_Daemon* daemon =
        MHD_start_daemon(MHD_USE_SELECT_INTERNALLY, config.producer_port, NULL,
                         NULL, &handler, NULL, MHD_OPTION_END);

    if (!daemon) {
        printf("Failed to start HTTP server\n");
        exit(1);
    }

    printf("Producer listening on port %d route %s\n", config.producer_port,
           config.producer_route);

    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);

    while (running) pause();

    MHD_stop_daemon(daemon);

    jsCtx_Destroy(js);
    natsConnection_Destroy(nc);

    return 0;
}
