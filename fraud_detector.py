from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, expr, to_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

# Initialize SparkSession with Kafka package
spark = SparkSession.builder \
    .appName("AdvancedRealTimeFraudDetection") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5")\
    .getOrCreate()

# Set log level to suppress INFO noise
spark.sparkContext.setLogLevel("WARN")

# Define schema for incoming Kafka JSON data
schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", StringType(), True),  # Keep as String for parsing
    StructField("location", StringType(), True)
])

# Read transactions from Kafka topic
transactions = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transaction-events") \
    .option("startingOffsets", "latest") \
    .load()

# Properly cast and parse Kafka 'value' as JSON
parsed = transactions.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp(col("timestamp")))

# -------------------------------
# 1️⃣ High Frequency Fraud Detection
# -------------------------------
high_freq_txns = parsed \
    .withWatermark("event_time", "10 seconds") \
    .groupBy(
        window(col("event_time"), "10 seconds"),
        col("user_id")
    ).agg(count("*").alias("txn_count")) \
    .filter(col("txn_count") > 5) \
    .select(
        col("user_id"),
        expr("'High Transaction Frequency' as reason")
    )

high_freq_output = high_freq_txns.selectExpr(
    "to_json(named_struct('user_id', user_id, 'reason', reason)) AS value"
)

high_freq_query = high_freq_output.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "fraud-alerts") \
    .option("checkpointLocation", "C:\\Users\\joshi\\Desktop\\DBT\\project\\checkpoints\\fraud-alerts-highfreq") \
    .outputMode("append") \
    .start()

# -------------------------------
# 2️⃣ Amount Anomaly Detection
# -------------------------------
fraud_by_amount = parsed.filter(col("amount") > 900) \
    .select(
        col("user_id"),
        col("transaction_id"),
        col("amount"),
        expr("'Unusual Amount Detected' as reason")
    )

amount_fraud_output = fraud_by_amount.selectExpr(
    "to_json(named_struct('user_id', user_id, 'transaction_id', transaction_id, 'amount', amount, 'reason', reason)) AS value"
)

amount_query = amount_fraud_output.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "fraud-alerts") \
    .option("checkpointLocation", "C:\\Users\\joshi\\Desktop\\DBT\\project\\checkpoints\\fraud-alerts-amount") \
    .start()

# -------------------------------
# 3️⃣ Write Valid Transactions
# -------------------------------
valid_txns = parsed.filter(col("amount") <= 900)

valid_output = valid_txns.selectExpr(
    "to_json(named_struct('transaction_id', transaction_id, 'user_id', user_id, 'amount', amount, 'timestamp', timestamp, 'location', location)) AS value"
)

valid_query = valid_output.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "valid-transactions") \
    .option("checkpointLocation", "C:\\Users\\joshi\\Desktop\\DBT\\project\\checkpoints\\valid-transactions") \
    .start()

# -------------------------------
# 🌟 Monitor Queries
# -------------------------------
for q in spark.streams.active:
    print(f"Running query: {q.name} | ID: {q.id}")

# Block until termination of any query
spark.streams.awaitAnyTermination()
