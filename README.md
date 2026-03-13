# C SMPP Messaging Gateway

**HTTP → NATS JetStream → SMPP → Redis → Logging → DLR Callback**

A lightweight **high-performance SMS gateway written in C** designed around an **event-driven architecture** using **NATS JetStream** as the message bus.

The system provides:

* HTTP SMS ingestion
* asynchronous processing via NATS
* multi-connection SMPP delivery
* delivery receipt tracking
* Redis message mapping
* structured logs
* DLR HTTP callbacks

The architecture is suitable for **high-throughput SMS processing pipelines**.

---

# Architecture

```
Client
  │
  │ HTTP POST /send_sms
  ▼
┌──────────────┐
│   Producer   │
│ HTTP Server  │
└──────┬───────┘
       │
       │ sms.outgoing
       ▼
┌────────────────┐
│  NATS STREAM   │
│      SMS       │
└──────┬─────────┘
       │
       │ Pull consumer
       ▼
┌──────────────────┐
│     Consumer     │
│   SMPP Workers   │
└──────┬───────────┘
       │
       │ SMPP
       ▼
      SMSC
       │
       ├─ submit_sm_resp
       │        │
       │        ▼
       │   sms.submit
       │
       └─ deliver_sm
                │
                ▼
           sms.delivery
                │
                ▼
           ┌─────────┐
           │ Logger  │
           └────┬────┘
                │
      ┌─────────┼───────────┐
      ▼         ▼           ▼
  submit.log delivery.log  Redis
                              │
                              ▼
                         DLR callback
```

---

# Components

## Producer

Location

```
producer/producer.c
```

Responsibilities

* HTTP server
* Accept SMS requests
* Publish to JetStream

Endpoint

```
POST /send_sms
```

Example request

```json
{
 "transaction_id":"TX123",
 "sender":"TEST",
 "destination":"1234567890",
 "message":"hello world"
}
```

Response

```
queued
```

Producer ensures the JetStream stream exists before publishing.

---

## Consumer

Location

```
consumer/consumer.c
```

Responsibilities

* Pull messages from NATS
* Send SMS via SMPP
* Publish submit and delivery events

Features

* multiple SMPP workers
* reconnect loop
* global TPS limiter
* submit response mapping
* delivery receipt parsing

Worker lifecycle

```
worker thread
    reconnect loop
        connect
        bind
        read SMPP PDUs
        reconnect on failure
```

---

## Logger

Location

```
logger/logger.c
```

Responsibilities

* process submit events
* process delivery events
* store message mapping in Redis
* write logs
* trigger DLR callbacks

---

# Logging

## Submit Log

```
timestamp|message_id|transaction_id|SUBMITTED|message
```

Example

```
2026-03-13T07:51:52Z|12345|TX123|SUBMITTED|hello
```

---

## Delivery Log

```
timestamp|message_id|transaction_id|status
```

Example

```
2026-03-13T07:52:10Z|12345|TX123|DELIVRD
```

---

# Redis Usage

Mapping stored as

```
sms:<message_id> → transaction_id
```

TTL

```
86400 seconds
```

Used for matching delivery receipts with original transactions.

---

# DLR Callback

Configured in

```
config/gateway.conf
```

Example

```
dlr_callback=http://localhost:9000/dlr
```

Payload

```json
{
 "transaction_id":"TX123",
 "message_id":"12345",
 "status":"DELIVRD"
}
```

---

# Configuration

File

```
config/gateway.conf
```

Example

```
producer_port=8080
producer_route=/send_sms

nats_url=nats://127.0.0.1:4222
nats_stream_name=SMS

nats_subject_outgoing=sms.outgoing
nats_subject_submit=sms.submit
nats_subject_delivery=sms.delivery

redis_host=127.0.0.1
redis_port=6379

smpp_host=127.0.0.1
smpp_port=2775

smpp_system_id=test
smpp_password=test
smpp_system_type=SMPP

smpp_workers=4
smpp_window_size=20

tps=100

dlr_callback=http://localhost:9000/dlr

log_dir=logs
```

---

# Directory Structure

```
.
├── producer/
│   └── producer.c
│
├── consumer/
│   └── consumer.c
│
├── logger/
│   └── logger.c
│
├── common/
│   ├── config.c
│   ├── config.h
│   ├── events.h
│   ├── nats_bus.c
│   └── redis_store.c
│
├── config/
│   └── gateway.conf
│
├── logs/
│   ├── submit.log
│   └── delivery.log
│
└── README.md
```

---

# Dependencies

Required libraries

```
libnats
libmicrohttpd
hiredis
libcurl
pthread
```

---

# Install Dependencies (Ubuntu)

```
sudo apt install \
libmicrohttpd-dev \
libhiredis-dev \
libcurl4-openssl-dev \
libnats-dev
```

---

# Build

Producer

```
gcc producer/producer.c common/config.c \
-lmicrohttpd -lnats -o producer-server
```

Consumer

```
gcc consumer/consumer.c common/config.c common/nats_bus.c \
-lnats -lpthread -o consumer-server
```

Logger

```
gcc logger/logger.c common/config.c \
-lcurl -lhiredis -lnats -o logger-server
```

---

# Run Order

Start services in this order

### 1 Start NATS

```
nats-server -js
```

### 2 Start Redis

```
redis-server
```

### 3 Start Producer

```
./producer-server
```

### 4 Start Consumer

```
./consumer-server
```

### 5 Start Logger

```
./logger-server
```

---

# Performance Control

Throughput is controlled using

```
tps=<value>
```

Example

```
tps=100
```

Consumer implements a **global TPS limiter** using a microsecond scheduler.

---

# Feature Status

| Feature                      | Status                | Notes                             |
| ---------------------------- | --------------------- | --------------------------------- |
| HTTP SMS API                 | ✔ Implemented         | Producer accepts POST requests    |
| JetStream event bus          | ✔ Implemented         | Stream auto created               |
| Persistent message queue     | ✔ Implemented         | JetStream file storage            |
| Pull-based consumer          | ✔ Implemented         | Consumer fetches batches          |
| Multiple SMPP binds          | ✔ Implemented         | `smpp_workers` config             |
| SMPP auto reconnect          | ✔ Implemented         | Worker reconnect loop             |
| Submit response handling     | ✔ Implemented         | `submit_sm_resp` parsed           |
| DLR parsing                  | ✔ Implemented         | `deliver_sm` processed            |
| Redis message mapping        | ✔ Implemented         | message_id → transaction_id       |
| Structured logging           | ✔ Implemented         | submit.log and delivery.log       |
| DLR HTTP callback            | ✔ Implemented         | via libcurl                       |
| Global TPS limiter           | ✔ Implemented         | nanosleep based scheduler         |
| Config file loader           | ✔ Implemented         | gateway.conf                      |
| JetStream stream auto-create | ✔ Implemented         | Producer ensures stream           |
| SMPP window size config      | ⚠ Pending enforcement | parameter exists but not enforced |
| SMPP enquire_link keepalive  | ⚠ Pending             | recommended for long sessions     |
| Submit retry logic           | ⚠ Pending             | currently no retry queue          |
| Dead letter queue            | ⚠ Pending             | possible via JetStream            |
| Metrics / monitoring         | ⚠ Pending             | Prometheus / stats endpoint       |
| Horizontal consumer scaling  | ⚠ Pending             | JetStream queue group scaling     |

---

# Reliability Characteristics

The system provides:

* asynchronous processing
* message persistence
* automatic SMPP reconnection
* delivery receipt tracking
* Redis state mapping

The design separates:

```
ingestion
processing
delivery
logging
```

which makes the gateway scalable and fault tolerant.

---

# Possible Future Enhancements

Recommended improvements for carrier-grade deployments:

* enforce SMPP window control
* implement enquire_link keepalive
* retry queue for failed submissions
* dead letter queue
* metrics and monitoring
* SMPP bind pools
* routing engine

