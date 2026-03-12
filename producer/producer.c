#include <microhttpd.h>
#include <nats/nats.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define PORT 8080
#define MAX_BODY 4096

natsConnection* nc;

typedef struct {
    char body[MAX_BODY];
    size_t size;
} RequestContext;

static enum MHD_Result handler(void* cls, struct MHD_Connection* connection,
                               const char* url, const char* method,
                               const char* version, const char* upload_data,
                               size_t* upload_data_size, void** con_cls) {
    RequestContext* ctx = *con_cls;

    if (strcmp(method, "POST") != 0) return MHD_NO;

    if (strcmp(url, "/send_sms") != 0) return MHD_NO;

    /* First call: allocate context */
    if (ctx == NULL) {
        ctx = calloc(1, sizeof(RequestContext));
        *con_cls = ctx;
        return MHD_YES;
    }

    /* Receive POST body chunks */
    if (*upload_data_size != 0) {
        if (ctx->size + *upload_data_size < MAX_BODY) {
            memcpy(ctx->body + ctx->size, upload_data, *upload_data_size);
            ctx->size += *upload_data_size;
        }

        *upload_data_size = 0;
        return MHD_YES;
    }

    /* Body fully received → publish to NATS */

    /*printf("Publishing: %.*s\n", (int)ctx->size, ctx->body);*/

    natsStatus s =
        natsConnection_Publish(nc, "sms.outgoing", ctx->body, ctx->size);

    if (s != NATS_OK) {
        printf("Publish failed: %s\n", natsStatus_GetText(s));
    } else {
        printf("Publish OK\n");
    }

    free(ctx);
    *con_cls = NULL;

    const char* response = "queued";

    struct MHD_Response* res = MHD_create_response_from_buffer(
        strlen(response), (void*)response, MHD_RESPMEM_PERSISTENT);

    enum MHD_Result ret = MHD_queue_response(connection, MHD_HTTP_OK, res);

    MHD_destroy_response(res);

    return ret;
}

int main() {
    natsStatus s;

    s = natsConnection_ConnectTo(&nc, "nats://localhost:4222");

    if (s != NATS_OK) {
        printf("NATS connect failed: %s\n", natsStatus_GetText(s));
        exit(1);
    }

    printf("Connected to NATS\n");

    struct MHD_Daemon* daemon =
        MHD_start_daemon(MHD_USE_SELECT_INTERNALLY, PORT, NULL, NULL, &handler,
                         NULL, MHD_OPTION_END);

    if (!daemon) {
        printf("Failed to start HTTP server\n");
        exit(1);
    }

    printf("Producer listening on %d\n", PORT);

    getchar();

    MHD_stop_daemon(daemon);

    natsConnection_Destroy(nc);

    return 0;
}
