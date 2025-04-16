from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda m: json.dumps(m).encode('utf-8')
)

def generate_transaction():
    return {
        "transaction_id": random.randint(100000, 999999),
        "user_id": random.randint(1, 500),
        "amount": round(random.uniform(1.0, 1000.0), 2),
        "timestamp": datetime.utcnow().isoformat(),
        "location": random.choice(["NY", "CA", "TX", "FL", "IL"])
    }

while True:
    txn = generate_transaction()
    producer.send("transaction-events", txn)
    print(f"Sent to transaction-events: {txn}")
    time.sleep(1)
