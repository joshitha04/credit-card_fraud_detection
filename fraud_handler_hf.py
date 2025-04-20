from kafka import KafkaConsumer
import json
import mysql.connector
from datetime import datetime

# Connect to MySQL
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="thrishal",
    database="fraud_detection"
)
cursor = db.cursor()

# Kafka Consumer for high-frequency fraud topic
consumer = KafkaConsumer(
    'fraud-alert-hf',
    bootstrap_servers=['localhost:9092'],
    group_id='hf-fraud-handler-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

for message in consumer:
    alert = message.value
    print(f"[HighFreq Fraud Handler] Received alert: {alert}")

    user_id = alert.get('user_id')
    reason = alert.get('reason', 'High Frequency Transactions Detected')  # Default if missing
    txn_count = alert.get('txn_count', 0)
    timestamp = alert.get('timestamp')  # Default if missing

    sql = """
    INSERT INTO frauds_hf (user_id, txn_count, timestamp, reason)
    VALUES (%s, %s, %s, %s)
    """
    values = (
        user_id,
        txn_count,
        timestamp,
        reason
    )

    try:
        cursor.execute(sql, values)
        db.commit()
        print(f"[HighFreq Fraud Handler] Alert saved for user {user_id}")
    except mysql.connector.Error as err:
        print(f"[HighFreq Fraud Handler] Database error: {err}")
        db.rollback()
