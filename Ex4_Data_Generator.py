from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql.functions import lit
import random
import uuid
import time
from datetime import datetime, timezone

# Create SparkSession with the application name
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("DataGenerator") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
    .getOrCreate()

# Input location (source data)
input_path = "s3a://spark/data/dims/cars"

# Output Kafka topic
kafka_topic = "sensors-sample"
kafka_bootstrap_servers = "course-kafka:9092"  

# Read cars data from S3/MinIO
cars_df = spark.read.parquet(input_path)

# Collect car_ids to driver side (since we generate events in a loop)
car_ids = [row["car_id"] for row in cars_df.select("car_id").collect()]

# Define schema for the events (Best Practice – avoid schema inference)
schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_time", TimestampType(), False),
    StructField("car_id", IntegerType(), False),
    StructField("speed", IntegerType(), False),
    StructField("rpm", IntegerType(), False),
    StructField("gear", IntegerType(), False),
])


# Infinite loop – generate data every second
while True:
    data = []

    # Generate one event per car (20 cars → 20 rows)
    for car_id in car_ids:
        event_id = str(uuid.uuid4())  # unique ID
        event_time = datetime.now(timezone.utc)
        speed = random.randint(0, 200)
        rpm = random.randint(0, 8000)
        gear = random.randint(1, 7)

        data.append((event_id, event_time, car_id, speed, rpm, gear))

    # Create DataFrame
    events_df = spark.createDataFrame(data, schema)

    # Convert rows to JSON
    json_df = events_df.selectExpr("to_json(struct(*)) AS value")

    # Write to Kafka
    json_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", kafka_topic) \
        .save()


    # Sleep for 1 second
    time.sleep(1)

# Close SparkSession when done 
spark.stop()
