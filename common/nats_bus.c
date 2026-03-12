
#include <nats/nats.h>
#include <stdio.h>

natsConnection* connect_nats(const char* url) {
    natsConnection* conn = NULL;
    natsStatus s;

    s = natsConnection_ConnectTo(&conn, url);

    if (s != NATS_OK) {
        printf("NATS connect failed: %s\n", natsStatus_GetText(s));
        return NULL;
    }

    return conn;
}

int publish_event(natsConnection* conn, const char* subject,
                  const char* payload) {
    natsStatus s;

    s = natsConnection_PublishString(conn, subject, payload);

    if (s != NATS_OK) {
        printf("Publish failed: %s\n", natsStatus_GetText(s));
        return -1;
    }

    return 0;
}

