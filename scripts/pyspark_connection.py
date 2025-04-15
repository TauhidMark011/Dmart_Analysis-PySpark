from pyspark.sql import SparkSession

# Function to create and return a SparkSession
def create_spark_sessions(app_name="Dmart Sales Analysis"):
    # Create or get an existing SparkSession
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate() # Create or get existing SparkSession
    return spark
