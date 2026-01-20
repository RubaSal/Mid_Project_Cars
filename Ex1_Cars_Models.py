from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Create SparkSession with the application name:
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("ModelCreation") \
    .getOrCreate()

# Define schema (Best Practice – avoid schema inference):
schema = StructType([
    StructField("model_id", IntegerType(), False),
    StructField("car_brand", StringType(), False),
    StructField("car_model", StringType(), False),
])

# Create the data:
data = [
    (1, "Mazda", "3"),
    (2, "Mazda", "6"),
    (3, "Toyota", "Corolla"),
    (4, "Hyundai", "i20"),
    (5, "Kia", "Sportage"),
    (6, "Kia", "Rio"),
    (7, "Kia", "Picanto"),
]

df = spark.createDataFrame(data, schema)

# Write to MinIO / S3a in Parquet format:
output_path = "s3a://spark/data/dims/car_models"

df.write \
  .mode("overwrite") \
  .format("parquet") \
  .save(output_path)

# Close SparkSession when done
spark.stop()
