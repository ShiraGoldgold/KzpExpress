import signal
import time

from src import RabbitMQConsumer, RedisClient, KafkaProducer

RETRY_DELAY = 5

redis_client = RedisClient(retry_delay=RETRY_DELAY)
rabbit_consumer = RabbitMQConsumer(retry_delay=RETRY_DELAY, queue_name='purchases_queue')
kafka_producer = KafkaProducer(retry_delay=RETRY_DELAY, topic_name='purchases_topic')


def shutdown_handler(sig, frame):
    print("Shutdown signal received")
    rabbit_consumer.stop()


def processing_logic(data):
    print(f"Processing purchase: {data.get('purchase_id')}")
    redis_client.store_data(data['purchase_id'], data, ttl_seconds=180)
    kafka_producer.send_event(data)
    time.sleep(100)
    print(f"Successfully sent purchase {data['purchase_id']} to Redis & Kafka")


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
        redis_client.close()
        kafka_producer.close()
        print("System stopped safely")


if __name__ == "__main__":
    main()