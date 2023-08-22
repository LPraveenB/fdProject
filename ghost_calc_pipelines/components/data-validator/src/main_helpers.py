import argparse
import concurrent.futures
import os
from datetime import datetime
from functools import partial
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, isnan, when, udf, collect_list, expr
import file_utils
import gcs_utils
import spark_utils
import uuid
from pyspark.sql import Row
from functools import reduce
import json
import ast

logger = None


def get_command_line_arguments():
    """
    The get_command_line_arguments function parses command-line arguments and assigns them to variables.

    :return: variables dict
    """
    try:
        # Parse command-line arguments
        parser = argparse.ArgumentParser(
            description='Data Validator')
        parser.add_argument('--date_missing_files',
                            dest='date_missing_files',
                            type=str,
                            required=True,
                            help='Date key and Missing File Names Value Dict')
        # GCS Bucket path from which the input files should be retrieved
        parser.add_argument('--input_path', dest='input_path', type=str, required=True, help='GCS Input Bucket Path')
        # GCS Bucket path where the error records files should be stored
        parser.add_argument('--error_path', dest='error_path', type=str, required=True, help='GCS Error Bucket Path')
        # GCS Bucket path where the report files should be stored
        parser.add_argument('--report_path', dest='report_path', type=str, required=True, help='GCS Report Bucket Path')
        parser.add_argument('--json_path', dest='json_path', type=str, required=True, help='GCS JSON File Path')

        args = parser.parse_args()

        # Assign command-line arguments to variables
        input_path = args.input_path
        error_path = args.error_path
        report_path = args.report_path
        json_path = args.json_path
        date_missing_files = ast.literal_eval(args.date_missing_files)

        return {
            "input_path": input_path,
            "error_path": error_path,
            "report_path": report_path,
            "json_path": json_path,
            "date_missing_files": date_missing_files
        }
    except Exception as e:
        print("Please pass all the required command line params : input_path, error_path, report_path, dates")
        print(f"Error while parsing command-line arguments : {e}")
        raise


def data_type_validation(df, df_copy: DataFrame, error_df: DataFrame, column_name: str, values: list,
                         report_df: DataFrame,
                         spark):
    """
    The data_type_validation function takes in a PySpark DataFrame, error_df, column_name, values, and report_df as input.
    It checks if the data type of the column matches with any of the values provided. If not, it returns an empty
    DataFrame and adds 1 to 'data_type' field in report df for that particular column.

    :param df_copy: Store the PySpark DataFrame that is being validated
    :param error_df: Store the rows which have errors in a separate DataFrame
    :param column_name: Specify the column name
    :param values: Specify the data type of the column
    :param report_df: Store the status of each column in a DataFrame
    """
    try:
        if not df_copy.isEmpty():
            df_copy_count = df_copy.count()
            try:
                for value in values:
                    check_validated = False
                    temp_df = df_copy.select(col(column_name))
                    temp_df = temp_df.withColumn(column_name, col(column_name).cast(value))
                    # print(f"temp_df : {column_name}: {value}")
                    # temp_df.show(truncate=False)
                    if temp_df.filter(col(column_name).isNull()).count() == df_copy_count:
                        error_df = df_copy.select("*")
                        df_copy = spark.createDataFrame([], schema=get_schema(df_copy.columns))
                        report_df = report_df.withColumn("data_type",
                                                         when(col("column") == column_name, df_copy_count).otherwise(
                                                             col("data_type")))
                        check_validated = True
                    if check_validated:
                        break
            except Exception as e:
                df_copy = spark.createDataFrame([], schema=get_schema(df_copy.columns))
                error_df = df_copy.select("*")
                report_df = report_df.withColumn("data_type",
                                                 when(col("column") == column_name, df_copy_count).otherwise(
                                                     col("data_type")))

        return df_copy, error_df, report_df

    except Exception as e:
        df_copy = spark.createDataFrame([], schema=get_schema(df_copy.columns))
        error_df = df_copy.select("*")
        report_df = report_df.withColumn("data_type",
                                         when(col("column") == column_name, df_copy_count).otherwise(
                                             col("data_type")))
        return df_copy, error_df, report_df


def null_check_validation(df, df_copy: DataFrame, error_df: DataFrame, column_name: str, values: list,
                          report_df: DataFrame, spark) -> tuple:
    try:
        null_condition = []
        not_null_condition = []
        original_null_condition = []

        if "null" in values:
            null_condition.append(df_copy[column_name].isNull())
            original_null_condition.append(df[column_name].isNull())
            values.remove("null")

        if values:
            null_condition.append(df_copy[column_name].isin(*values))
            not_null_condition.append(~df_copy[column_name].isin(*values))
            original_null_condition.append(df[column_name].isin(*values))

        original_df_null_or_empty = df.filter(reduce(lambda a, b: a | b, original_null_condition))

        # Separate rows with null or empty values in 'column_name' into one DataFrame
        df_with_null_or_empty = df_copy.filter(reduce(lambda a, b: a | b, null_condition))

        # Separate rows without null or empty values in 'column_name' into another DataFrame
        df_without_null_or_empty = df_copy.filter(reduce(lambda a, b: a & b, not_null_condition))

        df_copy = df_without_null_or_empty.select("*")
        error_df = error_df.union(df_with_null_or_empty)
        report_df = report_df.withColumn("null_check",
                                         when(col("column") == column_name,
                                              original_df_null_or_empty.count()).otherwise(
                                             col("null_check")))

        return df_copy, error_df, report_df
    except Exception as e:
        report_df = report_df.withColumn("null_check",
                                         when(col("column") == column_name,
                                              df_copy.count()).otherwise(
                                             col("null_check")))
        df_copy = spark.createDataFrame([], schema=get_schema(df_copy.columns))
        error_df = df_copy.select("*")
        return df_copy, error_df, report_df


# Define UDF to check date format
def check_date_format(date_str, date_formats):
    if date_str is None:
        return False

    for date_format in date_formats:
        try:
            datetime.strptime(date_str, date_format)
            return True
        except ValueError:
            continue
    return False


def date_format_validation(df, df_copy: DataFrame, error_df: DataFrame, column_name: str, values: list,
                           report_df: DataFrame, spark):
    try:
        check_date_format_udf = udf(lambda x: check_date_format(x, values), BooleanType())
        # Add a new column to DataFrame indicating if date format is correct
        df_original_with_date_format_check = df.withColumn("date_format_check",
                                                           col(column_name).isNull() |
                                                           col(column_name).isin("", " ") |
                                                           check_date_format_udf(col(column_name)))

        # Count the number of rows with a false date format check
        false_count_original = df_original_with_date_format_check.filter(
            df_original_with_date_format_check["date_format_check"] == False).count()

        df = df.drop("date_format_check")
        # Add a new column to DataFrame indicating if date format is correct
        df_with_date_format_check = df_copy.withColumn("date_format_check",
                                                       col(column_name).isNull() |
                                                       col(column_name).isin("", " ") |
                                                       check_date_format_udf(col(column_name)))

        # # Count the number of rows with a false date format check
        # false_count = df_with_date_format_check.filter(df_with_date_format_check["date_format_check"] == False).count()

        # Separate rows with wrong date format into a new DataFrame
        wrong_date_format_df = df_with_date_format_check.filter(~col("date_format_check"))

        # Separate rows with correct date format into another new DataFrame
        correct_date_format_df = df_with_date_format_check.filter(col("date_format_check"))

        wrong_date_format_df = wrong_date_format_df.drop("date_format_check")
        correct_date_format_df = correct_date_format_df.drop("date_format_check")

        df_copy = correct_date_format_df.select("*")
        error_df = error_df.union(wrong_date_format_df)
        report_df = report_df.withColumn("date_format",
                                         when(col("column") == column_name,
                                              false_count_original).otherwise(
                                             col("date_format")))
        return df_copy, error_df, report_df
    except Exception as e:
        report_df = report_df.withColumn("date_format",
                                         when(col("column") == column_name,
                                              df_copy.count()).otherwise(
                                             col("date_format")))
        df_copy = spark.createDataFrame([], schema=get_schema(df_copy.columns))
        error_df = df_copy.select("*")
        return df_copy, error_df, report_df


def upload_df_to_gcs(df: DataFrame, gcs_path: str, reference_file_name: str, file_name: str, destination_path):
    """
    The upload_df_to_gcs function uploads a PySpark DataFrame to GCS.

    :param df: Pass the PySpark DataFrame to be uploaded
    :param gcs_path: Specify the path to the GCS bucket
    :param reference_file_name: Create a folder in the GCS bucket to store the files
    :param file_name: Create the destination path in GCS
    """
    try:
        destination_path_final = f"{gcs_path.rstrip('/')}/{destination_path}.csv"
        df.coalesce(1).write.csv(destination_path_final, header=True, mode='overwrite')
        logger.info(f"File uploaded to : {destination_path_final}")
    except Exception as e:
        print(f"Error occurred while uploading df to GCS: {e}")
        raise


def get_schema(column_names):
    """
    Create a schema with the specified column names and StringType() as the data type.

    :param column_names: List of column names
    :return: Schema with the specified column names and StringType() as the data type
    """
    return StructType([StructField(name, StringType(), True) for name in column_names])


def generate_error_reference(df, error_df, gcs_uri, error_rows_df, spark):
    original_row_count = df.count()
    error_row_count = error_df.count()
    file_name = gcs_uri.split("/")[-2]
    file_date = gcs_uri.split("/")[-1].split("-")[-2]
    row_dict = {"file_date": file_date, "gcs_uri": gcs_uri, "file_name": file_name, "error_row_count": error_row_count,
                "original_row_count": original_row_count}
    row = Row(**row_dict)
    error_rows_df = error_rows_df.union(spark.createDataFrame([row], schema=error_rows_df.schema))
    return error_rows_df


def validate(df: DataFrame, json_data: list, report_columns: list, report_path: str, error_path: str,
             reference_file_name: str, file_name: str, spark, destination_path, gcs_uri, error_rows_df) -> DataFrame:
    try:
        df_copy = df.select("*")
        error_df = spark.createDataFrame([], schema=get_schema(df.columns))
        report_df = spark.createDataFrame([], schema=get_schema(report_columns))

        validations_col_dict = {f"{report_col}_validation": [] for report_col in report_columns[1:]}

        for each_col_dict in json_data:
            column_name = each_col_dict["name"]
            row_dict = {element: "0" for element in report_columns}
            row_dict['column'] = column_name
            # Convert the dictionary row to a Row object
            row = Row(**row_dict)
            report_df = report_df.union(spark.createDataFrame([row], schema=report_df.schema))

            for each_validation in each_col_dict["validations"]:
                validation_name = each_validation["name"]
                valid_values = each_validation["values"]
                valid_name = validation_name + '_validation'
                temp_val = validations_col_dict[valid_name]
                temp_val.append({'column': column_name, 'values': valid_values})
                validations_col_dict[valid_name] = temp_val

        for validation_key, validation_col_params in validations_col_dict.items():
            if validation_key in ["data_type_validation", "null_check_validation", "date_format_validation"]:
                # print(f"validation_key : {validation_key}")
                # print(f"validation_col_params : {validation_col_params}")
                for validation_col_param in validation_col_params:
                    validation_column_name = validation_col_param["column"]
                    validation_values = validation_col_param["values"]
                    df_copy, error_df, report_df = globals()[validation_key](df, df_copy, error_df,
                                                                             validation_column_name,
                                                                             validation_values, report_df, spark)
                    # print("df_copy")
                    # df_copy.show(truncate=False)
                    # print("error_df")
                    # error_df.show(truncate=False)
                    # print("report_df")
                    # report_df.show(truncate=False)
                    # print("************************************************")

        upload_df_to_gcs(error_df, error_path + "invalid_rows/", reference_file_name, file_name, destination_path)
        upload_df_to_gcs(report_df, report_path, reference_file_name, file_name, destination_path)
        error_rows_df = generate_error_reference(df, error_df, gcs_uri, error_rows_df, spark)
        return error_rows_df

    except Exception as e:
        print(f"Error occurred while validating: {e}")
        raise


def process_file(local_path, error_path, report_path, reference_file_name, json_data,
                 permitted_extensions, report_columns, spark, error_rows_df):
    """
    The process_file function takes in a local path to a file, an error_path, report_path and reference file name.
    It also takes in json data which is the schema of the reference file and permitted extensions for processing.
    The function then checks if the extension of the inputted local path is one of those permitted extensions. If it
    is not, the function will raise an exception stating that this extension cannot be processed by this program. If it
    does match one of these extensions then we create a spark session using our spark utils module and read in our csv
    or gz files as a pyspark

    :param local_path: Pass the path of the file to be processed
    :param error_path: Save the error files
    :param report_path: Save the report file
    :param reference_file_name: Get the reference file name
    :param json_data: Store the json schema
    :param permitted_extensions: Check if the file extension is supported or not
    :param report_columns: Specify the columns that are required in the report
    """
    try:
        if json_data:
            extension = local_path.split('.')[-1]
            file_name = local_path.split(r'/')[-1]
            # Extract the relevant parts of the path for the output
            destination_path = ('/'.join(local_path.split('/')[-2:])).split(".")[0]
            # print(f"destination_path : {destination_path}")
            # spark = SparkSession.builder.appName("Validator").getOrCreate()
            logger.info(f"Processing file : {local_path}")
            if extension in permitted_extensions:
                if extension == "csv" or extension == "gz":
                    pyspark_df = spark.read.csv(local_path, header=True)
                    error_rows_df = validate(pyspark_df, json_data, report_columns, report_path, error_path,
                                             reference_file_name, file_name, spark, destination_path, local_path,
                                             error_rows_df)
        return error_rows_df
    except Exception as e:
        print(f"Error occurred while processing file : {e}")
        raise


def filter_files_to_move(error_rows_df, missed_dates):
    print(missed_dates)
    files_to_move_to_threshold_error = []
    files_to_move_to_missed_files_threshold = []
    files_to_move_to_missed_files = []

    grouped_df = error_rows_df.groupBy("file_date", "file_name") \
        .agg(
        collect_list("gcs_uri").alias("gcs_uri"),
        F.sum("error_row_count").alias("error_row_count"),
        F.sum("original_row_count").alias("original_row_count")
    )
    # Calculate the new column as (other_column / another_col) * 100
    grouped_df = grouped_df.withColumn("calculation", expr("(error_row_count / original_row_count) * 100"))

    # Check if the values in the 'calculation' column are greater than 1, then set 1, otherwise 0
    grouped_df = grouped_df.withColumn("greater_than_1", when(col("calculation") > 1, 1).otherwise(0))

    for row in grouped_df.toLocalIterator():
        file_date = row["file_date"]
        gcs_uri = row["gcs_uri"]
        file_name = row["file_name"]
        greater_than_1 = row["greater_than_1"]
        if (file_date in missed_dates) and greater_than_1:
            files_to_move_to_missed_files_threshold.extend(gcs_uri)
        elif (file_date in missed_dates) and ~greater_than_1:
            files_to_move_to_missed_files.extend(gcs_uri)
        elif (file_date not in missed_dates) and greater_than_1:
            files_to_move_to_threshold_error.extend(gcs_uri)
    return files_to_move_to_missed_files, files_to_move_to_threshold_error, files_to_move_to_missed_files_threshold


def process_files(input_path, error_path, report_path, files, json_data, permitted_extensions, report_columns,
                  date_missing_files):
    """
    The process_files function is responsible for processing all files in the input_path.
    It will download each file from GCS, process it and upload the processed file to GCS.
    The function also generates a report of all processed files and uploads it to GCS.

    :param input_path: Specify the gcs bucket where input files are located
    :param error_path: Specify the path to store any files that fail processing
    :param report_path: Store the report file in gcs
    :param files: Specify the list of files to be processed
    :param json_data: Pass the json file contents to the process_file function
    :param permitted_extensions: Check the file extension of the downloaded files
    :param report_columns: Specify the columns to be included in the report file
    :return: The list of files that were processed
    """
    try:
        # all_files = gcs_utils.get_files_from_gcs_bucket(input_path)
        spark = SparkSession.builder.appName("Validator").getOrCreate()
        error_rows_df_schema = StructType([
            StructField("file_date", StringType(), nullable=True),
            StructField("gcs_uri", StringType(), nullable=True),
            StructField("file_name", StringType(), nullable=True),
            StructField("error_row_count", IntegerType(), nullable=True),
            StructField("original_row_count", IntegerType(), nullable=True)
        ])
        # error_rows_df_columns = ["file_date", "gcs_uri", "file_name", "error_row_count", "original_row_count"]
        error_rows_df = spark.createDataFrame([], schema=error_rows_df_schema)
        error_rows_df_clone = spark.createDataFrame([], schema=error_rows_df_schema)
        all_files = input_path.split(",")
        indent_list = json.dumps(all_files, indent=4)

        # Log the pretty JSON string
        logger.info("All files for processing : \n%s", indent_list)
        downloaded_paths = all_files
        all_params = []
        for downloaded_path in downloaded_paths:
            try:
                file_name = downloaded_path.split("/")[-1]
                for reference_file_name in files:
                    if reference_file_name in file_name:
                        try:
                            json_data_temp = json_data[reference_file_name]["columns"]
                        except KeyError:
                            json_data_temp = {}
                        all_params.append((downloaded_path, error_path, report_path, reference_file_name,
                                           json_data_temp, permitted_extensions, report_columns, spark, error_rows_df))
            except:
                continue

        for params in all_params:
            # print(*params)
            error_rows_df = process_file(*params)
            error_rows_df_clone = error_rows_df_clone.union(error_rows_df)

        destination_path_error_rows = f"{error_path.rstrip('/')}/analysis/analysis.csv"
        error_rows_df_clone.coalesce(1).write.csv(destination_path_error_rows, header=True, mode='overwrite')
        logger.info(f"File uploaded to : {destination_path_error_rows}")

        error_rows_df_clone.show(truncate=False)
        missed_dates = [key for key, value in date_missing_files.items() if value != "None"]
        files_to_move_to_missed_files, files_to_move_to_threshold_error, files_to_move_to_missed_files_threshold = filter_files_to_move(error_rows_df_clone,
                                                                                               missed_dates)
        print(f"files to move to missed files : {files_to_move_to_missed_files}")
        print(f"files to move to threshold error required files : {files_to_move_to_threshold_error}")
        print(f"files to move to threshold error missed files : {files_to_move_to_missed_files_threshold}")
        for gcs_uri_val in files_to_move_to_missed_files:
            gcs_uri_folder_path = "/".join(gcs_uri_val.split("/")[-2:])
            destination_uri = f"{error_path.rstrip('/')}/missed_files/{gcs_uri_folder_path}"
            gcs_utils.copy_file(gcs_uri_val, destination_uri)
            gcs_utils.delete_file_from_gcs(gcs_uri_val)

        for gcs_uri_value in files_to_move_to_threshold_error:
            gcs_uri_folder_path = "/".join(gcs_uri_value.split("/")[-2:])
            destination_uri = f"{error_path.rstrip('/')}/validation_threshold_error/required_files/{gcs_uri_folder_path}"
            gcs_utils.copy_file(gcs_uri_value, destination_uri)
            gcs_utils.delete_file_from_gcs(gcs_uri_value)

        for gcs_uri_value_fin in files_to_move_to_missed_files_threshold:
            gcs_uri_folder_path = "/".join(gcs_uri_value_fin.split("/")[-2:])
            destination_uri = f"{error_path.rstrip('/')}/validation_threshold_error/missed_files/{gcs_uri_folder_path}"
            gcs_utils.copy_file(gcs_uri_value_fin, destination_uri)
            gcs_utils.delete_file_from_gcs(gcs_uri_value_fin)

    except Exception as e:
        print(f"Error occurred while processing files : {e}")
        raise
