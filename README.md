# KzpExpress - Resilient Data Pipeline

This project implements a highly reliable and failure-tolerant data streaming pipeline. 
The system ensures Zero Data Loss and follows the At Least Once Delivery principle, 
guaranteeing that every purchase event generated is successfully processed, stored, and streamed.

## 🏗 System Architecture
1. *Generator (Producer):* Generates random purchase events and sends them to RabbitMQ.
2. *RabbitMQ:* Acts as the primary reliable message buffer.
3. *Consumer:* Orchestrates the flow - fetches from RabbitMQ, caches in Redis, and streams to Kafka.
4. *Redis:* Provides fast, temporary storage (TTL) for idempotency and quick lookups.
5. *Kafka:* The final destination for high-throughput downstream data analysis.

---

## Reliability & Resilience Mechanisms

### 1. Delivery Guarantees (At Least Once)
To ensure no message is ever lost, the system is designed around the At Least Once guarantee:
* *Manual Acknowledgments (ACKs):* The Consumer only sends an `ack` to RabbitMQ only after the data has been successfully stored in Redis AND acknowledged by Kafka. If any step fails, the message remains in RabbitMQ for retry.
* *Kafka Idempotence:* The Kafka Producer is configured with `enable.idempotence=True`. This ensures that even if a network retry occurs, Kafka won't store duplicate messages, maintaining data integrity.

### 2. Failure Tolerance & Infinite Retries
Every component (Redis, RabbitMQ, Kafka) inherits from a `BaseClient` that implements a robust Retry Loop:
* If a service goes down, the client "freezes" and attempts to reconnect every X seconds.
* *No Crash Policy:* The system does not crash on connection loss; it waits for the infrastructure to recover (Self-Healing).
### 3. Disaster Recovery & Persistence
* *RabbitMQ Durability:* Queues are marked as `durable=True` and messages as `delivery_mode=2` (persistent). Even if the RabbitMQ container restarts, the messages are recovered from the disk.
* *Kafka Acks (all):* The producer waits for all replicas to confirm the message before proceeding, ensuring the highest level of durability.

### 4. Graceful Shutdown
The system implements Signal Handling (SIGINT). When a shutdown is requested (Ctrl+C):
* The Consumer stops picking up new messages.
* The system finishes processing the current message.
* All connections (Rabbit, Redis, Kafka) are closed cleanly to avoid data corruption.

---

## Resilience Testing (Chaos Engineering)
We verified the system's hermeticity by intentionally breaking components during runtime:

| Component | Action | Expected Behavior | Result |
| :--- | :--- | :--- | :--- |
| **Redis** | `docker stop redis` | Consumer retries `store_data` infinitely. Message stays 'Unacked' in RabbitMQ. | ✅ Pass |
| **Kafka** | `docker stop kafka` | Kafka Producer retries delivery. Consumer waits and doesn't ACK RabbitMQ. | ✅ Pass |
| **RabbitMQ** | `docker stop rabbitmq` | Producer & Consumer enter reconnection loops. Data remains safe in memory/disk. | ✅ Pass |
| **Consumer** | `Hard Kill (Task Manager)` | RabbitMQ detects loss of connection and immediately requeues the message. | ✅ Pass |

---

## How to Run
1. Run Producer: `python producer_main.py` to start producing generated purchases to rabbit
2. Run Consumer: `python consumer_main.py` to start consuming messages from rabbit and sending them to redis and kafka.
