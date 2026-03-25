# KzpExpress - Enterprise Resilient Data Pipeline

A high-performance, failure-tolerant data streaming architecture designed for **Zero Data Loss** and real-time windowed analytics.

## 🏗 System Architecture
1. **Generator:** Produces real-time purchase events.
2. **RabbitMQ:** Acts as a persistent, durable buffer (At-Least-Once delivery).
3. **Bridge (R2K):** Orchestrates data flow from Rabbit to Kafka with manual ACKs.
4. **Analytics Pipeline (K2R):** Implements **Tumbling Window** logic and stores state in Redis.
5. **FastAPI:** Serves real-time "Hot Products" insights.

---

## 🛡 Performance & Scalability Deep-Dive

### 1. Atomic Operations & Race Conditions
To prevent data corruption during concurrent writes:
* **Redis Transactions:** We use `pipeline(transaction=True)` for `HSET` and `EXPIRE`. This guarantees that if a window key is created, it **will** have a TTL, preventing memory leaks.
* **Atomic Counters:** The system uses Redis's atomic nature to handle increments, ensuring that even under high load, the "Top Products" count remains accurate.

### 2. Scalability & High Throughput
* **Tumbling Window Partitioning:** By splitting data into time-based keys (e.g., `window:12:00-12:01`), we avoid "Hot Keys" and allow Redis to handle high-frequency writes efficiently.
* **Backpressure Control:** `prefetch_count=1` prevents consumer exhaustion. The pipeline only pulls new data when it has finished processing the current event.
* **Kafka Idempotency:** `enable.idempotence=True` ensures that retries don't result in duplicate data in the downstream analytics.

### 3. Resilience & Self-Healing
* **Infinite Retry Engine:** All clients (Kafka, Rabbit, Redis) inherit from `BaseClient`, providing a unified exponential backoff mechanism.
* **Graceful Degradation:** If the Analytics Service (Redis) is down, the API returns a `503 Service Unavailable` with a clear message, while the pipeline safely buffers data in RabbitMQ/Kafka.
* **Manual Acknowledgments:** Messages are only cleared from the source (RabbitMQ) after they are safely committed to the next destination (Kafka/Redis).

---

## 🧪 Failure Scenario Testing (Chaos Results)
| Component | Failure | System Behavior |
| :--- | :--- | :--- |
| **Redis** | Shutdown | Pipeline pauses, retries indefinitely. No data is ACKed in Rabbit. |
| **Kafka** | Network Cut | `flush()` fails -> NACK sent -> Message stays in RabbitMQ. |
| **Consumer** | `Ctrl+C` | `shutdown_handler` ensures the current task finishes before exit. |