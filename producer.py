from kafka import KafkaProducer
import json
import random
import time
from datetime import timedelta
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda m: json.dumps(m).encode('utf-8')
)
def generate_mysql_timestamp(base_time, offset_seconds):
    # Generate the timestamp in the format MySQL expects: YYYY-MM-DD HH:MM:SS
    timestamp = base_time + timedelta(seconds=offset_seconds)
    return timestamp.strftime("%Y-%m-%d %H:%M:%S")

# Base time for all timestamps
base_time = datetime(2025, 4, 20, 10, 0, 0)


locations = ["AP", "KA", "TS", "MH", "TN"]

transactions = []

# 5 high-frequency frauds for user_id=200
for i in range(15):
    transactions.append({
        "transaction_id": 200000 + i,
        "user_id": 200,
        "amount": round(random.uniform(1000, 5000), 2),
        "timestamp": generate_mysql_timestamp(base_time, i * 20),
        "location": "MH"
    })

# 15 high-amount frauds (>90000)
for i in range(25):
    transactions.append({
        "transaction_id": 210000 + i,
        "user_id": random.randint(201, 250),
        "amount": round(random.uniform(90001, 100000), 2),
        "timestamp": generate_mysql_timestamp(base_time, 600 + i * 20),
        "location": random.choice(locations)
    })

# 130 valid transactions
for i in range(130):
    transactions.append({
        "transaction_id": 220000 + i,
        "user_id": random.randint(1, 500),
        "amount": round(random.uniform(1.0, 90000.0), 2),
        "timestamp": generate_mysql_timestamp(base_time, 1000 + i * 15),
        "location": random.choice(locations)
    })

# Shuffle for randomness
random.shuffle(transactions)


# while True:
#     txn = generate_transaction()
#     producer.send("transaction-events", txn)
#     print(f"Sent to transaction-events: {txn}")
#     time.sleep(1)




for txn in transactions:
    producer.send("transaction-events", txn)
    print(f"✅ Sent to transaction-events: {txn}")
    time.sleep(1)  

producer.flush()
