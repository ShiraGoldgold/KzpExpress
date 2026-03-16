import signal
import time
from src import RabbitMQConsumer, KafkaProducer

RETRY_DELAY = 5
running = True

rabbit_consumer = RabbitMQConsumer(retry_delay=RETRY_DELAY, queue_name='purchases_queue')
kafka_producer = KafkaProducer(retry_delay=RETRY_DELAY, topic_name='purchases_topic')


def shutdown_handler(sig, frame):
    global running
    print("\nShutdown signal received. Finishing current task...")
    running = False
    rabbit_consumer.stop()


def processing_logic(data):
    print(f"Processing: {data.get('purchase_id')}")
    kafka_producer.send_event(data)


def main():
    signal.signal(signal.SIGINT, shutdown_handler)
    print("Consumer Loop started. Press Ctrl+C to stop.")
    while running:
        try:
            rabbit_consumer.consume(callback=processing_logic)
        except Exception as e:
            print(f"Error in main loop: {e}")
            time.sleep(RETRY_DELAY)
    print("Cleaning up resources...")
    rabbit_consumer.close()
    kafka_producer.close()
    print("System stopped safely.")


if __name__ == "__main__":
    main()