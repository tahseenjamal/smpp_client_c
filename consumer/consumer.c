#include <arpa/inet.h>
#include <nats/nats.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define SMPP_HOST "127.0.0.1"
#define SMPP_PORT 2775

#define SYSTEM_ID "test"
#define PASSWORD "test"
#define SYSTEM_TYPE ""

#define MAX_SEQ 10000

typedef struct {
    char txid[64];
    char text[512];
} SeqMap;

SeqMap seq_map[MAX_SEQ];

static uint32_t seq = 1;

int smpp_socket = -1;

uint32_t next_seq() { return seq++; }

void write_cstring(uint8_t* buf, int* pos, const char* str) {
    int len = strlen(str);
    memcpy(buf + *pos, str, len);
    *pos += len;
    buf[(*pos)++] = 0;
}

void send_pdu(uint8_t* pdu, int len) { write(smpp_socket, pdu, len); }

void smpp_connect() {
    struct sockaddr_in addr;

    smpp_socket = socket(AF_INET, SOCK_STREAM, 0);

    addr.sin_family = AF_INET;
    addr.sin_port = htons(SMPP_PORT);
    addr.sin_addr.s_addr = inet_addr(SMPP_HOST);

    if (connect(smpp_socket, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("SMPP connect");
        exit(1);
    }

    printf("SMPP connected\n");
}

void smpp_bind() {
    uint8_t pdu[1024];
    int pos = 16;

    write_cstring(pdu, &pos, SYSTEM_ID);
    write_cstring(pdu, &pos, PASSWORD);
    write_cstring(pdu, &pos, SYSTEM_TYPE);

    pdu[pos++] = 0x34;
    pdu[pos++] = 0;
    pdu[pos++] = 0;
    pdu[pos++] = 0;

    uint32_t len = pos;
    uint32_t seqno = next_seq();

    *(uint32_t*)(pdu) = htonl(len);
    *(uint32_t*)(pdu + 4) = htonl(0x00000009);
    *(uint32_t*)(pdu + 8) = 0;
    *(uint32_t*)(pdu + 12) = htonl(seqno);

    send_pdu(pdu, len);

    printf("bind_transceiver sent\n");
}

uint32_t smpp_submit(const char* from, const char* to, const char* msg,
                     const char* txid) {
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

    pdu[pos++] = 1;

    pdu[pos++] = 0;
    pdu[pos++] = 0;
    pdu[pos++] = 0;

    int len = strlen(msg);

    pdu[pos++] = len;
    memcpy(pdu + pos, msg, len);
    pos += len;

    uint32_t seqno = next_seq();

    *(uint32_t*)(pdu) = htonl(pos);
    *(uint32_t*)(pdu + 4) = htonl(0x00000004);
    *(uint32_t*)(pdu + 8) = 0;
    *(uint32_t*)(pdu + 12) = htonl(seqno);

    /* store mapping */

    strncpy(seq_map[seqno % MAX_SEQ].txid, txid, sizeof(seq_map[0].txid) - 1);
    strncpy(seq_map[seqno % MAX_SEQ].text, msg, sizeof(seq_map[0].text) - 1);

    send_pdu(pdu, pos);

    /*printf("submit_sm sent seq=%u tx=%s\n", seqno, txid);*/

    return seqno;
}

void on_sms(natsConnection* nc, natsSubscription* sub, natsMsg* msg,
            void* closure) {
    /*printf("Received: %.*s\n", natsMsg_GetDataLength(msg),*/
    /*natsMsg_GetData(msg));*/

    char txid[64] = {0};
    char sender[32] = {0};
    char dest[32] = {0};
    char text[512] = {0};

    sscanf(natsMsg_GetData(msg),
           "{\"transaction_id\":\"%63[^\"]\",\"sender\":\"%31[^\"]\","
           "\"destination\":\"%31[^\"]\",\"message\":\"%511[^\"]\"}",
           txid, sender, dest, text);

    smpp_submit(sender, dest, text, txid);

    natsMsg_Destroy(msg);
}

void smpp_read_loop(natsConnection* nc) {
    uint8_t header[16];

    while (1) {
        int r = read(smpp_socket, header, 16);
        if (r <= 0) break;

        uint32_t len = ntohl(*(uint32_t*)(header));
        uint32_t cmd = ntohl(*(uint32_t*)(header + 4));
        uint32_t seqno = ntohl(*(uint32_t*)(header + 12));

        uint8_t body[4096];
        read(smpp_socket, body, len - 16);

        /* submit_sm_resp */

        if (cmd == 0x80000004) {
            char message_id[64];

            strncpy(message_id, (char*)body, sizeof(message_id) - 1);
            message_id[sizeof(message_id) - 1] = 0;

            char* txid = seq_map[seqno % MAX_SEQ].txid;
            char* text = seq_map[seqno % MAX_SEQ].text;

            /*printf("submit_sm_resp seq=%u msgid=%s tx=%s\n", seqno,
             * message_id,*/
            /*txid);*/

            char json[1024];

            snprintf(json, sizeof(json),
                     "{\"message_id\":\"%s\",\"transaction_id\":\"%s\","
                     "\"message\":\"%s\"}",
                     message_id, txid, text);

            natsConnection_Publish(nc, "sms.submit", json, strlen(json));
        }

        /* deliver_sm */

        if (cmd == 0x00000005) {
            int pos = 0;

            pos += strlen((char*)body + pos) + 1;

            pos += 1;
            pos += 1;
            pos += strlen((char*)body + pos) + 1;

            pos += 1;
            pos += 1;
            pos += strlen((char*)body + pos) + 1;

            pos += 1;
            pos += 1;
            pos += 1;

            pos += strlen((char*)body + pos) + 1;
            pos += strlen((char*)body + pos) + 1;

            pos += 1;
            pos += 1;
            pos += 1;
            pos += 1;

            int sm_length = body[pos++];

            char dlr_text[512] = {0};
            memcpy(dlr_text, body + pos, sm_length);
            dlr_text[sm_length] = 0;

            /*printf("DLR TEXT: %s\n", dlr_text);*/

            char message_id[64] = {0};
            char status[32] = {0};

            char* p;

            p = strstr(dlr_text, "id:");
            if (p) sscanf(p + 3, "%63s", message_id);

            p = strstr(dlr_text, "stat:");
            if (p) sscanf(p + 5, "%31s", status);

            char json[256];

            snprintf(json, sizeof(json),
                     "{\"message_id\":\"%s\",\"status\":\"%s\"}", message_id,
                     status);

            natsConnection_Publish(nc, "sms.delivery", json, strlen(json));
        }
    }
}

int main() {
    natsConnection* nc = NULL;
    natsSubscription* sub = NULL;
    natsStatus s;

    smpp_connect();
    smpp_bind();

    s = natsConnection_ConnectTo(&nc, "nats://localhost:4222");

    if (s != NATS_OK) {
        printf("NATS connection failed\n");
        return 1;
    }

    s = natsConnection_Subscribe(&sub, nc, "sms.outgoing", on_sms, NULL);

    if (s != NATS_OK) {
        printf("Subscribe failed\n");
        return 1;
    }

    printf("Consumer running\n");

    smpp_read_loop(nc);

    return 0;
}
