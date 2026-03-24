import sys
import time
import json
import threading
from confluent_kafka import Producer, Consumer

# קונפיגורציה בסיסית
TOPIC = 'purchases_topic'
BOOTSTRAP_SERVERS = 'localhost:9092'

def run_stress_producer(producer_id, msg_count=10000):
    p = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS, 'linger.ms': 5})
    print(f"Producer {producer_id} started...")
    for i in range(msg_count):
        data = {"id": i, "producer": producer_id, "timestamp": time.time()}
        p.produce(TOPIC, value=json.dumps(data))
        if i % 1000 == 0: p.poll(0) # שחרור זכרון פנימי
    p.flush()
    print(f"Producer {producer_id} finished sending {msg_count} messages.")

def run_stress_consumer(consumer_id):
    c = Consumer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': 's1', # כולם באותה קבוצה!
        'auto.offset.reset': 'earliest'
    })
    c.subscribe([TOPIC])
    print(f"Consumer {consumer_id} started and waiting...")
    count = 0
    try:
        while True:
            msg = c.poll(1.0)
            if msg is None: continue
            count += 1
            if count % 1000 == 0:
                print(f"Consumer {consumer_id} processed {count} messages (from partition {msg.partition()})")
    except KeyboardInterrupt:
        c.close()


if __name__ == "__main__":
    run_stress_producer(sys.argv[1])
# איך לבדוק?
# 1. תפתחי טרמינל 1, 2, 3: תריצי בכל אחד run_stress_consumer (תראי איך כל אחד מקבל פרטישן אחר)
# 2. תפתחי טרמינל 4, 5: תריצי בכל אחד run_stress_producer ותראי איך העומס מתחלק.