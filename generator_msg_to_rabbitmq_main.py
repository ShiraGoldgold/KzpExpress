import random
import signal
import time
from src import RabbitMQProducer, create_random_purchase

RETRY_DELAY = 5
running = True

rabbit_producer = RabbitMQProducer(retry_delay=RETRY_DELAY, queue_name='purchases_queue')


def shutdown_handler(sig, frame):
    global running
    print("\nShutdown signal received. Generator stopping...")
    running = False
    rabbit_producer.stop()


def main():
    signal.signal(signal.SIGINT, shutdown_handler)
    print("Producer Generator Loop started. Press Ctrl+C to stop.")
    while running:
        try:
            rabbit_producer.publish(create_random_purchase())
            time.sleep(random.uniform(1, 5))
        except Exception as e:
            print(f"Error in main loop: {e}")
            time.sleep(RETRY_DELAY)
    print("Cleaning up resources...")
    rabbit_producer.close()
    print("System stopped safely.")


if __name__ == "__main__":
    main()