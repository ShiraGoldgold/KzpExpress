# 🚀 KzpExpress - Enterprise Resilient Data Pipeline

## 📌 Overview

KzpExpress is a high-performance, failure-resilient data streaming pipeline designed for 
**Zero Data Loss**. The system processes real-time purchase events using a multi-stage 
architecture that guarantees delivery even under extreme resource failure, network 
partitions, or service crashes.

## 🏗 System Architecture

The pipeline follows a **"Buffer-Stream-Cache"** pattern:

* **Generator**: Produces random purchase events
* **RabbitMQ (Ingestion)**: Acts as the primary durable entry point
* **Bridge (R2K)**: Moves data from RabbitMQ to Kafka with safety checks
* **Analytics (K2R)**: Processes windowed data from Kafka into Redis
* **Insights API**: Serves real-time top-product analytics

## 🛡 Reliability & Data Hermeticity (No Message Lost)

We have implemented a strict **At-Least-Once delivery guarantee** across the entire stack.

### 1. Ingestion Layer (RabbitMQ)

To ensure messages survive even if the RabbitMQ broker crashes:

* **Durable Queues**: Declared with `durable=True` in `rabbitmq_base.py`
* **Persistent Messaging**: Every message is published with `delivery_mode=2` (Persistent)
* **Manual Acknowledgments**: Consumers only acknowledge (`basic_ack`) after the message 
    is safely handed off to the next component

### 2. Streaming Layer (Kafka)

To guarantee consistency and prevent data loss during transfer:

* **Idempotent Producer**: Configured with `enable.idempotence=True` to prevent duplicates 
    during retries
* **Full Acknowledgments**: `acks='all'` ensures all replicas have received the message 
    before the producer continues
* **Transaction Safety**: The Bridge (RabbitMQ Consumer) only sends an Ack to RabbitMQ
    after the Kafka Producer confirms delivery via the `_delivery_report` callback

### 3. Storage Layer (Redis)

* **Write-Ahead Logging**: Configured with `appendonly=yes` and `appendfsync=always` to
    ensure every write is immediately flushed to disk
* **Atomic Operations**: Uses Redis Pipelines to ensure updates are processed correctly

## 🔄 Failure Tolerance & Disaster Recovery

We tested the system by "killing" components via Docker during active streams. Here is 
how the system handles disasters:

| Failure Scenario  | Impact                         | Mitigation Strategy                                                                                                                          | Evidence in Code               |
|-------------------|--------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------|
| **RabbitMQ Down** | Producer cannot send events    | Looped Retry with Backoff: The BaseClient enters a retry loop until the broker is back, or the client stops the terminal                     | `_run_with_retry`              |
| **Kafka Down**    | Bridge cannot forward messages | Message Requeueing: Messages remain Unacked in RabbitMQ. When the bridge fails to send to Kafka, it triggers a nack and requeues the message | `_callback_wrapper`            |
| **Redis Down**    | Analytics cannot save windows  | Offset Retention: Kafka offsets are NOT committed until the Redis write is successful. The consumer will retry the same message upon restart | `consume_and_act_realtime_msg` |
| **Service Crash** | Process stops mid-task         | Graceful Shutdown: SIGINT handlers ensure the system finishes the current message and closes connections properly                            | `shutdown_handler`             |

## ⚡ Scalability & Performance

### Handling High Load

* **Pressure Control**: Used `prefetch_count=1` in RabbitMQ to prevent a single consumer from 
    being overwhelmed by a flood of messages
* **Tumbling Windows**: Processing logic distributes analytics load into 1-minute buckets, 
    preventing memory spikes
* **Non-Blocking IO**: Kafka producers use `poll(0)` to handle delivery reports without 
    blocking the main event loop

### Race Conditions

* **Atomic Retries**: The `_run_with_retry` pattern in the BaseClient ensures that connection 
    attempts do not overlap or cause race conditions during resource recovery
* **TTL Management**: Redis keys are set with `nx=True` during expiration updates to prevent 
   multiple clients from overwriting TTLs inconsistently

## 📡 API Reference

### Get Hot Products

**Endpoint**: `GET /hot-products-last-1-minutes-window`

**Sample Response**:
```json
{
  "window": "window:14:02-14:03",
  "data": [
    {"name": "Coffee", "count": 85},
    {"name": "Cookies", "count": 42}
  ]
}
```

## 🚀 Running the Project

### 1. Start Infrastructure
```bash
docker-compose up -d
```

# In separate terminals:
python generator_msg_to_rabbitmq.py
python rabbitmq_to_kafka.py
python kafka_to_redis.py
python api_main.py