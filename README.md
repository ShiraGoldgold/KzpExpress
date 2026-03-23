# KzpExpress - Resilient Data Pipeline

This project implements a highly reliable and failure-tolerant data streaming pipeline. 
The system ensures **Zero Data Loss** and follows the **At Least Once Delivery** principle, 
guaranteeing that every purchase event is successfully processed, cached, and streamed.

## 🏗 System Architecture
1. **Generator (Producer):** Generates random purchase events and sends them to RabbitMQ.
2. **RabbitMQ:** Acts as the primary reliable message buffer with persistent queues.
3. **Consumer (Orchestrator):** Fetches from RabbitMQ, stores in Redis for windowing/analytics, and streams to Kafka.
4. **Redis:** Provides fast storage for tumbling window calculations with TTL-based expiration.
5. **Kafka:** The final destination for high-throughput downstream data analysis.

---

## 🛡 Reliability & Resilience Mechanisms

### 1. Delivery Guarantees (At Least Once)
To ensure no message is ever lost, the system is designed around the "At Least Once" guarantee:
* **Manual Acknowledgments (ACKs):** RabbitMQ is configured to wait for a manual ACK. The consumer only sends this ACK after the data has been successfully processed by the next hop (Kafka/Redis).
* **Synchronous Kafka Production:** We use `producer.flush()` after every message. This blocks the RabbitMQ ACK until Kafka confirms the message is physically stored in the broker.
* **Idempotent Producer:** Kafka is configured with `enable.idempotence=True` to prevent duplicate messages in case of network retries.

### 2. Failure Tolerance & Connection Recovery
Every component (Redis, RabbitMQ, Kafka) inherits from a `BaseClient` that manages connection lifecycles:
* **Infinite Retries:** If a broker (Kafka/Rabbit/Redis) goes down, the system enters a retry loop, printing clear logs, and waits for the service to recover without crashing.
* **Prefetch Control:** `prefetch_count=1` is used in RabbitMQ to prevent the consumer from being overwhelmed and to ensure sequential, reliable processing.
* **Persistence:** RabbitMQ queues are marked as `durable`, ensuring messages survive a RabbitMQ service restart.

### 3. Graceful Shutdown & Signaling

The system implements robust signal handling (`SIGINT`):
* Upon `Ctrl+C`, the system finishes the **current** task and ensures the last message is fully processed (or requeued) before closing connections.
* **Double-Interrupt Protection:** The system handles the edge case where a blocking `flush()` or `poll()` in the underlying C library might delay the shutdown, ensuring resources are always cleaned up.

### 4. Network & Connectivity Optimizations

To ensure high performance and reduce latency, we implemented several low-level optimizations:

* **IPv4 Forced Resolution:** Configured Kafka clients with `broker.address.family: v4` to prevent connection delays caused by unsuccessful IPv6 resolution attempts on local environments.
* **Fast Failure Detection:** Adjusted Kafka's `session.timeout.ms` and `heartbeat.interval.ms` to ensure the cluster detects consumer failures within seconds, enabling faster rebalancing.
* **Aggressive Redis Timeouts:** Implemented `socket_connect_timeout` in the Redis client to prevent the pipeline from blocking indefinitely during network partitions.
* **Unified Retry Logic:** All external service interactions are inherited from a centralized `BaseClient`, providing consistent exponential backoff and reconnection logic across the entire infrastructure.

---

## 🧪 Resilience Testing (Chaos Engineering)
We verified the system's hermeticity by intentionally breaking components during runtime:

| Component | Action | Expected Behavior | Result |
| :--- | :--- | :--- | :--- |
| **Redis** | `docker stop redis` | Consumer retries `add_to_hset` infinitely. Message stays 'Unacked' in RabbitMQ. | ✅ Pass |
| **Kafka** | `docker stop kafka` | `flush()` hits timeout -> Exception raised -> RabbitMQ `NACK` & Requeue. | ✅ Pass |
| **RabbitMQ** | `docker stop rabbitmq` | Producer & Consumer enter reconnection loops. Data remains safe on disk. | ✅ Pass |
| **Network Interruption** | Disconnect WiFi | Producers block and retry until connection is restored. No data loss. | ✅ Pass |


---

## 🚀 How to Run
1. **Infrastructure:** Start the stack using `docker-compose up -d` (RabbitMQ, Kafka, Redis).
2. **Generator:** `python generator_msg_to_rabbitmq_main.py`
3. **Primary Pipeline:** `python rabbitmq_to_kafka_main.py`
4. **Analytics/Redis:** `python kafka_to_redis_main.py`