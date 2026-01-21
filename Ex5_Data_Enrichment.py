from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, round, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Create SparkSession with the application name
spark = SparkSession.builder \
    .appName("DataEnrichment") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
    .getOrCreate()

# Streaming read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "course-kafka:9092") \
    .option("subscribe", "sensors-sample") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON
schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("car_id", IntegerType(), True),
    StructField("speed", IntegerType(), True),
    StructField("rpm", IntegerType(), True),
    StructField("gear", IntegerType(), True)
])

df_parsed = df.selectExpr("CAST(value AS STRING) as json_str") \
              .select(from_json(col("json_str"), schema).alias("data")) \
              .select("data.*")

# Load enrichment tables (still batch from S3)
Cars = spark.read.parquet("s3a://spark/data/dims/cars")
CarModels = spark.read.parquet("s3a://spark/data/dims/car_models")
CarColors = spark.read.parquet("s3a://spark/data/dims/car_colors")

# Join + enrichment
Data_Enrichment = (
    df_parsed
    .join(Cars, on="car_id", how="inner")
    .join(CarModels, on="model_id", how="inner")
    .join(CarColors, on="color_id", how="inner")
    .withColumnRenamed("car_model","model_name")
    .withColumnRenamed("car_brand","brand_name")
    .withColumn("expected_gear",(round(col("speed") / 30)).cast("int"))
    .select("event_id","event_time","car_id","speed","rpm","gear","driver_id","brand_name","model_name","color_name","expected_gear")
)

# Convert to JSON for Kafka
Data_Enrichment_to_kafka = Data_Enrichment.select(to_json(struct("*")).alias("value"))

# Streaming write to Kafka
query = Data_Enrichment_to_kafka.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "course-kafka:9092") \
    .option("topic", "samples-enriched") \
    .option("checkpointLocation", "/tmp/spark_checkpoint_enriched") \
    .start()

query.awaitTermination()  # keep streaming alive