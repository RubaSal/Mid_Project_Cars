from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql.functions import col
import random

# Create SparkSession with the application name:
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("CarsGenerator") \
    .getOrCreate()

def CarsGenerator(spark, num_records=20, output_path="s3a://spark/data/dims/cars"):
    """
    Generate cars data and save to S3/MinIO.
    """



    # Generate unique 7-digit car_ids
    car_ids = random.sample(range(1_000_000, 10_000_000), num_records)

    # Generate unique 9-digit driver_ids
    driver_ids = random.sample(range(100_000_000, 1_000_000_000), num_records)

    data = []
    for car_id, driver_id in zip(car_ids, driver_ids):
        model_id = random.randint(1, 7)  # between 1 and 7
        color_id = random.randint(1, 7)  # between 1 and 7
        data.append((car_id, driver_id, model_id, color_id))

    # Define schema (Best Practice – avoid schema inference)
    schema = StructType([
        StructField("car_id", IntegerType(), False),
        StructField("driver_id", IntegerType(), False),
        StructField("model_id", IntegerType(), False),
        StructField("color_id", IntegerType(), False),
    ])

    df = spark.createDataFrame(data, schema)

    # Write to MinIO / S3a in Parquet format
    df.write \
      .mode("overwrite") \
      .format("parquet") \
      .save(output_path)

    return df


# Call the function to generate and save the cars data
CarsGenerator(spark)

# Close SparkSession when done
spark.stop()
