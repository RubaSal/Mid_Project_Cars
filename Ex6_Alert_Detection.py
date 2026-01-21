from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql.functions import col, from_json, to_json, struct


# Create SparkSession with the application name
spark = SparkSession.builder \
    .appName("AlertDetection") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
    .getOrCreate()

# Streaming read from Kafka - samples_enriched
df_samples_enriched = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "course-kafka:9092") \
    .option("subscribe", "samples-enriched") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# Parse JSON - samples_enriched
schema_samples_enriched = StructType([
    StructField("driver_id", IntegerType(), True),
    StructField("brand_name", StringType(), True),
    StructField("model_name", StringType(), True),
    StructField("color_name", StringType(), True),
    StructField("expected_gear", IntegerType(), True),
    StructField("event_id", StringType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("car_id", IntegerType(), True),
    StructField("speed", IntegerType(), True),
    StructField("rpm", IntegerType(), True),
    StructField("gear", IntegerType(), True)
])

samples_enriched = df_samples_enriched.selectExpr("CAST(value AS STRING) as json_str") \
              .select(from_json(col("json_str"), schema_samples_enriched).alias("data")) \
              .select("data.*")


# alerting
Alerting_Detection = (
    samples_enriched
    .filter(
        (col("speed") > 120) |
        (col("expected_gear") != col("gear")) |
        (col("rpm") > 6000)
    )
    .select("*")
)


# Convert to JSON for Kafka
Alerting_Detection_to_kafka = Alerting_Detection.select(to_json(struct("*")).alias("value"))

# Streaming write to Kafka
alert_stream = Alerting_Detection_to_kafka.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "course-kafka:9092") \
    .option("topic", "alert-data") \
    .option("checkpointLocation", "/tmp/spark_checkpoint_alert") \
    .start()

alert_stream.awaitTermination()  # keep streaming alive
