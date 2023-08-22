import argparse
from google.cloud import storage
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import split
import sys_
import os
from resources import config
from utils import *


# Set spark environments
os.environ['PYSPARK_PYTHON'] = '/opt/conda/default/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3'


def get_spark_session():
    """
    generate the spark session object
    """
    app_name = "Preprocessing_Job"
    spark_object = SparkSession.builder.appName(app_name).getOrCreate()
    return spark_object


print('Preprocessing Job Started..')

spark = get_spark_session()


def create_location_groups_and_save(key, file_paths, output_path):
    # Initialize an empty DataFrame
    df: DataFrame = None
    for file_path in file_paths:
        if file_path.endswith(".csv"):
            # Read .csv files into DataFrame
            if df is None:
                df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
            else:
                temp_df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
                df = df.union(temp_df)
        elif file_path.endswith(".gz"):
            # Read .gz files into DataFrame
            if df is None:
                df = spark.read.text(file_path)
            else:
                temp_df = spark.read.text(file_path)
                df = df.union(temp_df)
    if df is not None:
        df = df.withColumn('LOCATION_GROUP',
                           f.round((f.col('LOCATION') / 50), 0))  # Add the "extra" column with value 0
        df.write.option("header", True).option("compression", "gzip").mode("overwrite") \
            .partitionBy("LOCATION_GROUP").parquet(output_path + "/" + key + '.parquet')

    # Dictionary to hold the map of key and list of values


def preprocessing(spark, input_paths, output_path, master_output_path, date_list):
    """
    processing the output of validator data , create location groups in it and save to output folder
    """
    result_map = {}
    for item in input_paths:
        # Split the value using "/"
        split_values = item.split("/")
        # Extract the value at index -2
        key = split_values[-2]
        # Add the item to the corresponding list in the dictionary
        if key not in result_map:
            result_map[key] = []
        result_map[key].append(item)
    print(result_map)

    # Loop through the map and create DataFrames for each key and respective list of values
    for key, file_paths in result_map.items():
        # if loop will directly move the master files into master folder
        if key in ["FD_SKU_MASTER", "FD_CARRIER_DETAILS", "FD_LOCATION"]:
            print(f"moving master file {key} to master location")
            spark.read.options(header='True', inferSchema='True') \
                .csv(file_paths).write.option("header", True).option("compression", "gzip").mode("overwrite") \
                .parquet(master_output_path + "/" + key + '.parquet')
            continue
        create_location_groups_and_save(key, file_paths, output_path)


def main(args=None):
    print("args", args)
    parser = argparse.ArgumentParser(description='Denormalizer Process')

    parser.add_argument(
        '--input_path',
        dest='input_path',
        type=str,
        required=True,
        help='Path with data to be processed for that particular day (this are the succuss files from data-validator)')

    parser.add_argument(
        '--output_path',
        dest='output_path',
        type=str,
        required=True,
        help='Path where data will be saved after preprocessing.')

    parser.add_argument(
        '--master_output_path',
        dest='master_output_path',
        type=str,
        required=True,
        help='Path where data will be saved after preprocessing.')

    parser.add_argument(
        '--dates',
        dest='dates',
        type=str,
        required=True,
        help='dates to be processed')

    args = parser.parse_args(args)

    print('args.input_path', args.input_path)
    print('args.output_path', args.output_path)
    print('args.master_output_path', args.master_output_path)
    print('args.dates', args.dates)
    spark = get_spark_session()
    # Convert the comma-separated string to a list
    input_paths = args.input_path.split(',')
    date_list = args.dates.split(',')
    preprocessing(spark, input_paths, args.output_path, args.master_output_path, date_list)


if __name__ == '__main__':
    main()

