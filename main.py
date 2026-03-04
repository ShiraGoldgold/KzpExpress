import time
import random
from src import RabbitMQProducer, create_random_purchase


def main():
    producer = RabbitMQProducer()
    try:
        producer.connect()
        print("Generator started- press Ctrl+C to stop")
        while True:
            new_purchase = create_random_purchase()
            producer.publish(new_purchase)
            wait_time = random.uniform(1, 5)
            time.sleep(wait_time)
    except KeyboardInterrupt:
        print("Generator stop")
    finally:
        producer.close()


if __name__ == "__main__":
    main()