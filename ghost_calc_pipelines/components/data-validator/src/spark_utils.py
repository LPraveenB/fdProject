from pyspark.sql import SparkSession
import os

logger = None


def create_spark_session():
    try:
        return SparkSession.builder.appName("Validator").getOrCreate()
    except Exception as e:
        print(f"Error occurred while creating spark session : {e}")
        raise
