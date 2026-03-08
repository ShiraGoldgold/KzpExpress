import signal
from src import RabbitMQConsumer
from src import RedisClient

redis_client = RedisClient(retry_delay=5)
rabbit_consumer = RabbitMQConsumer(retry_delay=5, queue_name='purchases_queue')


def shutdown_handler(sig, frame):
    print("Shutdown signal received")
    rabbit_consumer.stop()


def processing_logic(data):
    print(f"Processing purchase: {data.get('purchase_id')}")
    redis_client.store_data(data['purchase_id'], data, ttl_seconds=180)


def main():
    signal.signal(signal.SIGINT, shutdown_handler)
    try:
        print("Consumer started- Press Ctrl+C to stop.")
        rabbit_consumer.consume(callback=processing_logic)
    except Exception as e:
        print(f"Fatal error: {e}")
    finally:
        print("Final cleanup")
        rabbit_consumer.close()
        # redis_client.close()
        print("System stopped safely")


if __name__ == "__main__":
    main()