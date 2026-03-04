import time
import random
from src import RabbitMQProducer, create_random_purchase


def main():
    producer = RabbitMQProducer()
    try:
        print("Generator started- press Ctrl+C to stop")
        while True:
            success = producer.publish(create_random_purchase())
            if not success:
                print("Could not recover. Skipping this purchase.")
            time.sleep(random.uniform(1, 5))
    except KeyboardInterrupt:
        print("Generator stop")
    finally:
        producer.close()


if __name__ == "__main__":
    main()