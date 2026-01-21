from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Create SparkSession with the application name
spark = SparkSession.builder \
    .appName("AlertCounter") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# Schema
schema = StructType([
    StructField("event_id", StringType()),
    StructField("event_time", TimestampType()),
    StructField("car_id", IntegerType()),
    StructField("driver_id", IntegerType()),
    StructField("brand_name", StringType()),
    StructField("model_name", StringType()),
    StructField("color_name", StringType()),
    StructField("speed", IntegerType()),
    StructField("rpm", IntegerType()),
    StructField("gear", IntegerType()),
    StructField("expected_gear", IntegerType())
])


# Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers",  "course-kafka:9092") \
    .option("subscribe", "alert-data") \
    .option("startingOffsets", "earliest") \
    .load()


# Parse JSON
parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp("event_time", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))


# Windowed Aggregation (15 minutes)
agg_df = parsed_df \
    .withWatermark("event_time", "15 minutes") \
    .groupBy(window(col("event_time"), "15 minutes")) \
    .agg(
        count("*").alias("num_of_rows"),
        sum(when(lower(col("color_name")) == "black", 1).otherwise(0)).alias("num_of_black"),
        sum(when(lower(col("color_name")) == "white", 1).otherwise(0)).alias("num_of_white"),
        sum(when(lower(col("color_name")) == "silver", 1).otherwise(0)).alias("num_of_silver"),
        max("speed").alias("maximum_speed"),
        max("gear").alias("maximum_gear"),
        max("rpm").alias("maximum_rpm")
    )


# 6. Output to console
query = agg_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()

