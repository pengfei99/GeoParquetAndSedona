from pyspark.sql import SparkSession, DataFrame
import requests
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf, col, split


# Create a SparkSession
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Example PySpark Job") \
    .getOrCreate()

# converted centroid parquet file path
fr_zone_file_path = "/home/pliu/data/converted_centroid_of_french_commune"
converted_centroid_df = spark.read.parquet(fr_zone_file_path)
converted_centroid_df = converted_centroid_df.repartition(16)
converted_centroid_df.cache()
converted_centroid_df.show(5)

# Stop the SparkSession
spark.stop()
