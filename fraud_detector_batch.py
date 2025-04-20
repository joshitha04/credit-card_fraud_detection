from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, expr, to_timestamp, window

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("BatchFraudDetection") \
    .config("spark.jars", "C:\\Users\\joshi\\Desktop\\DBT\\project\\mysql-connector-j-9.3.0.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# JDBC connection properties
jdbc_url = "jdbc:mysql://localhost:3306/fraud_detection"
connection_properties = {
    "user": "root",
    "password": "thrishal",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Load transaction data from MySQL (same schema as streaming)
transactions = spark.read.jdbc(
    url=jdbc_url,
    table="all_transactions",  # Make sure this table exists
    properties=connection_properties
)

# Convert timestamp column to actual timestamp type
transactions = transactions.withColumn("event_time", to_timestamp("timestamp"))

# -------------------------------
# 1️⃣ High Frequency Fraud Detection
# -------------------------------
high_freq = transactions \
    .groupBy(window(col("event_time"), "5 minutes"), col("user_id")) \
    .agg(count("*").alias("txn_count")) \
    .filter(col("txn_count") > 3) \
    .select(col("user_id"), expr("'High Transaction Frequency' as reason"))

# -------------------------------
# 2️⃣ Amount Anomaly Detection
# -------------------------------
amount_anomalies = transactions \
    .filter(col("amount") > 90000) \
    .select("user_id", "transaction_id", "amount") \
    .withColumn("reason", expr("'Unusual Amount Detected'"))

# -------------------------------
# 3️⃣ Valid Transactions
# -------------------------------
valid_txns = transactions \
    .filter(col("amount") <= 90000)

# -------------------------------
# 4️⃣ Store Results for Comparison
# -------------------------------
high_freq.write.jdbc(
    url=jdbc_url,
    table="batch_fraud_highfreq",
    mode="overwrite",  # or "append" based on use case
    properties=connection_properties
)

amount_anomalies.write.jdbc(
    url=jdbc_url,
    table="batch_fraud_amount",
    mode="overwrite",
    properties=connection_properties
)

valid_txns.write.jdbc(
    url=jdbc_url,
    table="batch_valid_transactions",
    mode="overwrite",
    properties=connection_properties
)

print("✅ Batch fraud detection completed successfully.")
