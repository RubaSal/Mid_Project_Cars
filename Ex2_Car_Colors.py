from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Create SparkSession with the application name:
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("ColorCreation") \
    .getOrCreate()

# Define schema (Best Practice – avoid schema inference):
schema = StructType([
    StructField("color_id", IntegerType(), False),
    StructField("color_name", StringType(), False),
])

# Create the data:
data = [
    (1, "Black"),
    (2, "Red"),
    (3, "Gray"),
    (4, "White"),
    (5, "Green"),
    (6, "Blue"),
    (7, "Pink"),
]

df = spark.createDataFrame(data, schema)

# Write to MinIO / S3a in Parquet format:
output_path = "s3a://spark/data/dims/car_colors"

df.write \
  .mode("overwrite") \
  .format("parquet") \
  .save(output_path)

# Close SparkSession when done
spark.stop()
