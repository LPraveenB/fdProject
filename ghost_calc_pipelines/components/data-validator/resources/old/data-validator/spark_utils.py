from pyspark.sql import SparkSession
import os

logger = None


def create_spark_session():
    try:
        return SparkSession.builder.master("local").getOrCreate()
    except Exception as e:
        print(f"Error occurred while creating spark session : {e}")
        raise
