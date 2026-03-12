Below is a clean **`README.md`** you can place at the root of your repository. It documents architecture, build steps, run instructions, and log formats for your SMS pipeline.

# C SMS Gateway (NATS + SMPP)

A lightweight high-performance SMS gateway implemented in **C**, using:

* **NATS** as the message bus
* **SMPP** for telecom connectivity
* **Redis** for transaction mapping
* **libmicrohttpd** for HTTP ingestion

The system is designed to handle **very high SMS throughput** with minimal dependencies.

---

# Architecture

```
HTTP Client
    │
    ▼
Producer (HTTP API)
    │
    ▼
NATS subject: sms.outgoing
    │
    ▼
Consumer (SMPP worker)
    │
    ├── submit_sm → SMSC
    │
    ├── submit_sm_resp
    │        │
    │        ▼
    │     NATS sms.submit
    │
    └── deliver_sm
             │
             ▼
        NATS sms.delivery
             │
             ▼
            Logger
             │
             ├── Redis lookup
             │
             ├── logs/submit.log
             └── logs/delivery.log
```

---

# Components

## Producer

HTTP API that accepts SMS submissions and publishes them to NATS.

Endpoint:

```
POST /send_sms
```

Example request:

```json
{
  "transaction_id": "TX12345",
  "sender": "TEST",
  "destination": "12345",
  "message": "hello"
}
```

Producer publishes the request to:

```
sms.outgoing
```

---

## Consumer (SMPP Worker)

The consumer:

1. Subscribes to `sms.outgoing`
2. Sends SMS via SMPP `submit_sm`
3. Tracks SMPP sequence → transaction mapping
4. Publishes events

Events emitted:

```
sms.submit
sms.delivery
```

---

## Logger

The logger subscribes to:

```
sms.submit
sms.delivery
```

It:

* Stores `message_id → transaction_id` mapping in Redis
* Writes submission and delivery logs

---

# Log Format

## submit.log

```
datetime|message_id|transaction_id|submission_status|message
```

Example:

```
2026-03-12T08:12:37Z|2|TX12345|SUBMITTED|hello
```

---

## delivery.log

```
datetime|message_id|transaction_id|delivery_status
```

Example:

```
2026-03-12T08:12:39Z|2|TX12345|DELIVRD
```

---

# Dependencies

Required libraries:

* NATS C client
* hiredis
* libmicrohttpd
* Redis server
* NATS server

Mac install example (Homebrew):

```
brew install nats-server hiredis libmicrohttpd redis
```

---

# Build Instructions

### Producer

```
gcc producer/producer.c -o producer_server \
$(pkg-config --cflags --libs libmicrohttpd) \
-I/usr/local/include \
-L/usr/local/lib \
-lnats \
-Wl,-rpath,/usr/local/lib
```

---

### Consumer

```
gcc consumer/*.c common/*.c -o consumer-server \
-I/opt/homebrew/include \
-L/opt/homebrew/lib \
-I/usr/local/include \
-L/usr/local/lib \
-lnats \
-lhiredis \
-Wl,-rpath,/opt/homebrew/lib \
-Wl,-rpath,/usr/local/lib
```

---

### Logger

```
gcc logger/logger.c -o logger-server \
-I/opt/homebrew/include \
-L/opt/homebrew/lib \
-I/usr/local/include \
-L/usr/local/lib \
-lnats \
-lhiredis \
-Wl,-rpath,/opt/homebrew/lib \
-Wl,-rpath,/usr/local/lib
```

---

# Running the System

Start components in the following order.

### 1. Start Redis

```
redis-server
```

### 2. Start NATS

```
nats-server
```

### 3. Start Logger

```
./logger-server
```

### 4. Start Consumer

```
./consumer-server
```

### 5. Start Producer

```
./producer_server
```

---

# Send SMS

Example request:

```
curl -X POST http://localhost:8080/send_sms \
-d '{"transaction_id":"TX12345","sender":"TEST","destination":"12345","message":"hello"}'
```

Response:

```
queued
```

---

# Project Structure

```
producer/
    producer.c

consumer/
    consumer.c

logger/
    logger.c

common/
    events.h
    nats_bus.c
    redis_store.c
```

---

# Performance Notes

The system uses:

* **NATS async messaging**
* **C SMPP implementation**
* **Minimal memory allocation**
* **Redis for fast lookup**

This architecture can handle **tens of thousands of SMS per second per node**.

---

# Future Improvements

Possible production enhancements:

* multi-threaded SMPP workers
* persistent message queues
* HTTP delivery callbacks
* rate limiting / TPS control
* horizontal scaling

