#ifndef EVENTS_H
#define EVENTS_H

#define MAX_TX 64
#define MAX_ADDR 32
#define MAX_MSG 512

typedef struct {
    char transaction_id[MAX_TX];
    char sender[MAX_ADDR];
    char destination[MAX_ADDR];
    char message[MAX_MSG];

} SmsOutgoingEvent;

typedef struct {
    char transaction_id[MAX_TX];
    char message_id[MAX_TX];
    char sender[MAX_ADDR];
    char destination[MAX_ADDR];
    char message[MAX_MSG];

} SubmitEvent;

typedef struct {
    char message_id[MAX_TX];
    char status[32];
    char text[256];
    char error[32];

} DlrEvent;

#endif

