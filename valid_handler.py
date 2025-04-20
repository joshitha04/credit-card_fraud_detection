from kafka import KafkaConsumer
import json
import mysql.connector

# Connect to MySQL
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="thrishal",
    database="fraud_detection"
)
cursor = db.cursor()

consumer = KafkaConsumer(
    'valid-transactions',
    bootstrap_servers=['localhost:9092'],
    group_id='valid-handler-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

for message in consumer:
    txn = message.value
    print(f"[Valid Handler] Saving to DB: {txn}")
    sql = "INSERT INTO transactions (transaction_id, user_id, amount, timestamp, location) VALUES (%s, %s, %s, %s, %s)"
    values = (
        txn['transaction_id'],
        txn['user_id'],
        txn['amount'],
        txn['timestamp'].replace("T", " ").split(".")[0],
        txn['location']
    )
    try:
        cursor.execute(sql, values)
        db.commit()
    except mysql.connector.IntegrityError:
        print(f"Duplicate valid transaction skipped: {txn['transaction_id']}")
