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

consumer = KafkaConsumer(
    'fraud-alerts',
    bootstrap_servers=['localhost:9092'],
    group_id='fraud-handler-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

for message in consumer:
    fraud_alert = message.value
    print(f"[Fraud Handler] Received alert: {fraud_alert}")
    
    # The fraud alerts might have different structures based on the detection method
    user_id = fraud_alert.get('user_id')
    reason = fraud_alert.get('reason')
    
    # Some alerts might have transaction_id, amount, etc.
    transaction_id = fraud_alert.get('transaction_id', 0)  # Default if missing
    amount = fraud_alert.get('amount', 0.0)  # Default if missing
    
    # Generate a timestamp if not present
    current_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    
    sql = "INSERT INTO frauds (transaction_id, user_id, amount, timestamp, location) VALUES (%s, %s, %s, %s, %s)"
    values = (
        transaction_id,
        user_id,
        amount,
        current_time,
        "UNKNOWN"  # Location might not be available in fraud alerts
    )
    
    try:
        cursor.execute(sql, values)
        db.commit()
        print(f"[Fraud Handler] Alert saved for user {user_id}: {reason}")
    except mysql.connector.Error as err:
        print(f"Database error: {err}")
        if err.errno == 1062:  # Duplicate entry
            print(f"Duplicate fraud record skipped: {transaction_id}")
        db.rollback()