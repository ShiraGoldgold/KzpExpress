# KzpExpress - Resilient & Hermetic Data Pipeline

This project implements a highly reliable, failure-tolerant data streaming pipeline designed for **Zero Data Loss**. The architecture follows the **At Least Once Delivery** principle, ensuring every purchase event is processed, indexed, and stored even under extreme infrastructure instability.

## 🏗 System Architecture
1. **Generator (Producer):** Produces purchase events and streams them to RabbitMQ.
2. **RabbitMQ:** Acts as the persistent message buffer (Primary Ingress).
3. **Rabbit-to-Kafka Consumer:** Orchestrates the flow from RabbitMQ to Kafka.
4. **Kafka-to-Redis Consumer:** Processes streaming data from Kafka into Time-Windowed Redis Hashes.
5. **Redis:** High-speed storage for real-time analytics (Top 3 items per window).
6. **Kafka:** The central nervous system for high-throughput, idempotent data distribution.

---

## 🛡 Reliability & Resilience Mechanisms

### 1. Delivery Guarantees (At Least Once)
The system is built to ensure that no message is ever "lost in flight":
* **Manual Offset Management:** In the `KafkaConsumer`, `enable.auto.commit` is set to `False`. The offset is committed ONLY after the message is successfully processed by the callback (e.g., stored in Redis).
* **RabbitMQ Acknowledgments:** Messages remain in the queue (Unacked) until the entire downstream flow (Kafka delivery) is confirmed.
* **Kafka Idempotence:** The Kafka Producer uses `enable.idempotence=True` and `acks=all`. This prevents duplicate writes during network retries and ensures data is replicated across all brokers.

### 2. Failure Tolerance (The BaseClient Pattern)
Every service (Redis, RabbitMQ, Kafka) inherits from a robust `BaseClient` abstract class:
* **Infinite Retry Loop:** If a connection drops, the client enters a non-blocking retry state, attempting to reconnect every 5 seconds.
* **Self-Healing:** The system does not crash; it "freezes" and waits for the infrastructure (Docker containers) to recover, then resumes exactly where it left off.
* **Connection Validation:** Uses `list_topics` (Kafka) and `ping` (Redis) to verify actual connectivity before attempting operations.

### 3. Disaster Recovery & Persistence
* **RabbitMQ Durability:** Queues are `durable=True` and messages are `persistent` (delivery_mode=2), surviving broker restarts.
* **Redis Idempotency:** By using `HSET` with `purchase_id` as a field, we ensure that duplicate message processing (inherent to At-Least-Once) results in a "no-op" write, maintaining 100% data integrity.
* **Time-to-Live (TTL):** Data in Redis is automatically cleaned up after a defined window, preventing memory exhaustion.

### 4. Graceful Shutdown
The system implements OS signal handling (`SIGINT`):
* Upon receiving a shutdown signal, the system finishes the **current** processing task.
* It performs a `close()` operation on all clients, flushing buffers and closing sockets cleanly to prevent corrupted offsets or lingering connections.

---

## 🧪 Chaos Engineering (Resilience Testing)
We verified the system's hermeticity by intentionally killing components during peak load:

| Component | Action | System Response | Recovery |
| :--- | :--- | :--- | :--- |
| **Kafka** | `docker stop kafka` | Consumer detects loss via `list_topics`, enters Retry loop. | Once Kafka is up, Consumer resumes and commits pending offsets. |
| **Redis** | `docker stop redis` | Consumer fails the `HSET` operation, retries infinitely without committing Kafka offset. | Data is written to Redis only once it recovers; no data is lost. |
| **RabbitMQ** | `docker stop rabbit` | Producer enters reconnection loop. All pending data stays in the Generator's retry buffer. | Flow resumes immediately upon RabbitMQ recovery. |

---

## 🚀 How to Run
1. **Tear up Infrastructure:** `docker-compose up -d` (Redis, Kafka, RabbitMQ).
2. **Start Producer:** `python generator_msg_to_rabbitmq_main.py`
3. **Start Bridge:** `python rabbitmq_to_kafka_main.py`
4. **Start Analytics:** `python kafka_to_redis_main.py`