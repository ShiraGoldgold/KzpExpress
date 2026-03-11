Reliability & Resilience
~~~~~~~~~~~~~~~~~~~~~~~~~Step 1: Producer~~~~~~~~~~~~~~~~~~~~~~~~~

To ensure the At Least Once Delivery principle and prevent data loss (Zero Data Loss), the following mechanisms were
implemented:
    1. Publisher Confirms
        By using confirm_delivery(), the channel is set to a synchronous mode. The Producer will not proceed to the
        next message until it receives a positive acknowledgment (ACK) from RabbitMQ, confirming the current message
        has been safely received.
    2. Durability & Persistence
        a. Queue Durability: The queue is declared with durable=True, ensuring that the queue definition survives a
           broker restart.
        b. Message Persistence: Every message is published with delivery_mode=2. This forces RabbitMQ to persist the
           message to the disk rather than keeping it only in RAM.
    3. Infinite Retry Mechanism (Failure Tolerance)
        The publish method implements an infinite while True loop. In the event of a network exception or if the broker
        is unreachable:
        a. The error is caught via Exception Handling.
        b. Connections are safely cleaned up (close).
        c. The system waits for a constant defined period (DEFAULT_RETRY_SECONDS_DELAY) and attempts to reconnect and
           resend the exact same message.
           Result: The Generator "freezes" during downtime, guaranteeing that no purchase event is lost.
    4. Resilience Testing Performed
        a. Broker Down at Startup: The system waits and retries connection attempts until the RabbitMQ service becomes
           available.
        b. Broker Shutdown During Runtime: The system detects the lost stream, initiates the retry logic, and
           successfully delivers the pending message as soon as the container is back online.

~~~~~~~~~~~~~~~~~~~~~~~~~Step 2: Consumer~~~~~~~~~~~~~~~~~~~~~~~~~

To maintain the At Least Once Delivery guarantee and ensure Disaster Recovery during the message processing phase, the
following architectural patterns were applied:
    1. Manual Acknowledgement (ACK/NACK)
        a. The consumer is configured with auto_ack=False. An acknowledgement is only sent to RabbitMQ after the
           processing logic (storing in Redis/Kafka) is successfully completed.
        b. Failure Handling: If the processing fails (e.g., database downtime), the system catches the exception and
           sends a basic_nack with requeue=True. This ensures the message is returned to the queue for a future retry
           rather than being lost.
    2. QoS (Quality of Service) - Prefetch Count
        We use prefetch_count=1. This prevents a single consumer from being overwhelmed by multiple messages and ensures
        that if a consumer crashes, only one message at most will need to be re-processed.
    3. Graceful Shutdown
        a. The system utilizes Signal Handling (SIGINT) to capture termination requests (Ctrl+C).
        b. Upon receiving a signal, the consumer invokes stop_consuming(), allowing it to finish processing the current
            message and send the final ACK before closing the connection. This prevents "partial processing" or "zombie
            messages."
    4. Self-Healing Infrastructure
        Like the Producer, the Consumer inherits from a BaseClient that implements a recursive retry logic. If the
        connection to RabbitMQ or Redis is severed, the consumer enters a "wait-and-retry" state until the infrastructure
        is recovered.

Resilience Testing (Chaos Engineering Principles)
    To verify the Failure Tolerance of the system, we performed the following "Chaos" tests:
        1. Redis Downtime
            Action: Executed docker stop redis.
            Expected Behavior: The consumer remains active and initiates an infinite retry loop for the store_data
            operation. The current message in RabbitMQ stays in an "Unacked" state, ensuring it is not lost.
            Result: ✅ Pass
        2. RabbitMQ Broker Shutdown
            Action: Executed docker stop rabbitmq.
            Expected Behavior: Both the Producer and Consumer freeze their current execution and enter connection-retry
            mode. No data is lost from the generator's memory or the processing pipeline.
            Result: ✅ Pass
        3. Consumer Hard Kill
            Action: Performed a Hard Kill (SIGKILL) on the Consumer process.
            Expected Behavior: RabbitMQ detects the abrupt connection loss. It immediately moves the "Unacked" message
            back to a "Ready" state, making it available for the next consumer to pick up.
            Result: ✅ Pass
        4. Network Instability
            Action: Simulated a network disconnection (Disconnect Internet/VPN).
            Expected Behavior: The BaseClient catches the socket-level error and initiates the predefined retry mechanism
            with the configured delay, recovering automatically once the connection is restored.
            Result: ✅ Pass