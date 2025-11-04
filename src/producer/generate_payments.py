#!/usr/bin/env python3
"""Simple Kafka payments producer for the capstone project.

Generates JSON payment events and publishes to the `payments.raw` topic.
"""
import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer


fake = Faker()


def random_transaction():
    txn = {
        "transaction_id": f"TXN{uuid.uuid4().hex[:12].upper()}",
        "ts_event": datetime.now(timezone.utc).isoformat(),
        "card_hash": uuid.uuid4().hex[:16],
        "merchant_id": f"M{random.randint(10000,99999)}",
        "amount": round(random.uniform(1.0, 20000.0), 2),
        "currency": random.choice(["USD", "EUR", "GBP", "JPY"]),
        "mcc": random.choice(["5411", "5812", "5999", "5814"]),
        "channel": random.choice(["POS", "WEB", "MOBILE"]),
        "auth_result": random.choice(["APPROVED", "DECLINED"]),
        "location": f"{fake.city()}, {fake.country()}"
    }
    return txn


def main(bootstrap_server: str, topic: str, rate: float, total: int, seed: int):
    if seed is not None:
        random.seed(seed)
        Faker.seed(seed)

    producer = KafkaProducer(
        bootstrap_servers=[bootstrap_server],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        linger_ms=5,
    )

    sent = 0
    interval = 1.0 / rate if rate > 0 else 0
    try:
        while total <= 0 or sent < total:
            evt = random_transaction()
            # Occasionally emit a malformed event to exercise deadletter routing
            if random.random() < 0.02:
                # drop a required field
                evt.pop('transaction_id', None)

            producer.send(topic, evt)
            sent += 1
            if sent % 100 == 0:
                print(f"Sent {sent} events")
            if interval > 0:
                time.sleep(interval)

    except KeyboardInterrupt:
        print("Stopping producer")
    finally:
        producer.flush()
        producer.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--bootstrap-server', default='localhost:9092')
    parser.add_argument('--topic', default='payments.raw')
    parser.add_argument('--rate', type=float, default=10.0, help='events per second')
    parser.add_argument('--total', type=int, default=0, help='total events to send (0 = infinite)')
    parser.add_argument('--seed', type=int, default=None)
    args = parser.parse_args()
    main(args.bootstrap_server, args.topic, args.rate, args.total, args.seed)
