import signal
import time
from src import KafkaConsumer, RedisClient
from datetime import timedelta, datetime

RETRY_DELAY = 5
running = True
THUMBLING_WINDOW_MINUTES = 1

kafka_consumer = KafkaConsumer(retry_delay=RETRY_DELAY, topic_name='purchases_topic',
                               group_id='purchases_group')
redis_client = RedisClient(retry_delay=RETRY_DELAY)


def shutdown_handler(sig, frame):
    global running
    print("\nShutdown signal received. Finishing current task...")
    running = False
    kafka_consumer.stop()
    redis_client.stop()


def get_thumbling_window_key(purchase_time):
    total_minutes = purchase_time.hour * 60 + purchase_time.minute
    start_of_window_minutes = ((total_minutes // THUMBLING_WINDOW_MINUTES)
                               * THUMBLING_WINDOW_MINUTES)
    window_start = purchase_time.replace(
        hour=start_of_window_minutes // 60, minute=start_of_window_minutes % 60,
        second=0, microsecond=0)
    window_end = window_start + timedelta(minutes=THUMBLING_WINDOW_MINUTES)
    return f"window:{window_start.strftime('%H:%M')}-{window_end.strftime('%H:%M')}"


def processing_data_to_redis(data):
    print(f"Processing: {data.get('purchase_id')}")
    purchase_time = datetime.strptime(data.get('purchase_time'), "%Y-%m-%d %H:%M:%S.%f")
    key = get_thumbling_window_key(purchase_time)
    if not redis_client.add_to_hset(key=key,
                             data_field=data.get('purchase_id'),
                             data_value=data.get('item_id'),
                             ttl_seconds=THUMBLING_WINDOW_MINUTES * 3 * 60):
            raise Exception("Error sending data to kafka")
    print(f"Successfully stored msg in Redis key: {key}")


def main():
    signal.signal(signal.SIGINT, shutdown_handler)
    print("Consumer Loop started. Press Ctrl+C to stop.")
    while running:
        try:
            kafka_consumer.consume_and_act_realtime_msg(callback=processing_data_to_redis)
        except Exception as e:
            print(f"Error in main loop: {e}")
            time.sleep(RETRY_DELAY)
    print("Cleaning up resources...")
    kafka_consumer.close()
    redis_client.close()
    print("System stopped safely.")


if __name__ == "__main__":
    main()