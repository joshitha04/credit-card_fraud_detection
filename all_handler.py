from kafka import KafkaConsumer
import json
import mysql.connector

# Kafka Consumer to read all transactions
consumer = KafkaConsumer(
    'transaction-events',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='all-transactions-group'
)

# MySQL connection
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="thrishal",
    database="fraud_detection"
)
cursor = db.cursor()

# Insert SQL
insert_sql = """
INSERT INTO all_transactions (transaction_id, user_id, amount, timestamp, location)
VALUES (%s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
    user_id=VALUES(user_id),
    amount=VALUES(amount),
    timestamp=VALUES(timestamp),
    location=VALUES(location)
"""

print("🟢 Listening to all transactions...")

for message in consumer:
    txn = message.value
    try:
        cursor.execute(insert_sql, (
            txn["transaction_id"],
            txn["user_id"],
            txn["amount"],
            txn["timestamp"],
            txn["location"]
        ))
        db.commit()
        print(f"✅ Inserted into all_transactions: {txn}")
    except Exception as e:
        print(f"❌ Error inserting into DB: {e}")
        db.rollback()
