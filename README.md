Reliability & Resilience (Step 1: Producer)
To ensure the At Least Once Delivery principle and prevent data loss (Zero Data Loss), the following mechanisms were implemented:
    1. Publisher Confirms
        By using confirm_delivery(), the channel is set to a synchronous mode. The Producer will not proceed to the next message
        until it receives a positive acknowledgment (ACK) from RabbitMQ, confirming the current message has been safely received.
    2. Durability & Persistence
        a. Queue Durability: The queue is declared with durable=True, ensuring that the queue definition survives a broker restart.
        b. Message Persistence: Every message is published with delivery_mode=2. This forces RabbitMQ to persist the message to the
           disk rather than keeping it only in RAM.
    3. Infinite Retry Mechanism (Failure Tolerance)
        The publish method implements an infinite while True loop. In the event of a network exception or if the broker is unreachable:
        a. The error is caught via Exception Handling.
        b. Connections are safely cleaned up (close).
        c. The system waits for a constant defined period (DEFAULT_RETRY_SECONDS_DELAY) and attempts to reconnect and resend the
           exact same message.
           Result: The Generator "freezes" during downtime, guaranteeing that no purchase event is lost.
    4. Resilience Testing Performed
        a. Broker Down at Startup: The system waits and retries connection attempts until the RabbitMQ service becomes available.
        b. Broker Shutdown During Runtime: The system detects the lost stream, initiates the retry logic, and successfully delivers
           the pending message as soon as the container is back online.