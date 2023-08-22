import argparse
import concurrent.futures
import os
from datetime import datetime
from functools import partial

import pandas as pd
from pyspark.sql.types import *

import file_utils
import gcs_utils
import spark_utils

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
        # GCS Bucket path from which the input files should be retrieved
        parser.add_argument('--input_path', dest='input_path', type=str, required=True, help='GCS Input Bucket Path')
        # GCS Bucket path where the error records files should be stored
        parser.add_argument('--error_path', dest='error_path', type=str, required=True, help='GCS Error Bucket Path')
        # GCS Bucket path where the report files should be stored
        parser.add_argument('--report_path', dest='report_path', type=str, required=True, help='GCS Report Bucket Path')
        parser.add_argument('--json_path', dest='json_path', type=str, required=True, help='GCS JSON File Path')
        parser.add_argument('--date_missing_files',
                            dest='date_missing_files',
                            type=str,
                            required=True,
                            help='Date key and Missing File Names Value Dict')

        args = parser.parse_args()

        # Assign command-line arguments to variables
        input_path = args.input_path
        error_path = args.error_path
        report_path = args.report_path
        json_path = args.json_path
        date_missing_files = args.date_missing_files

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


def data_type_validation(df_copy, error_df, column_name, values, report_df):
    """
    The data_type_validation function takes in a dataframe, error_df, column_name, values and report_df as input.
    It checks if the data type of the column matches with any of the values provided. If not it returns an empty
    dataframe and adds 1 to 'data_type' field in report df for that particular column.

    :param df_copy: Store the dataframe that is being validated
    :param error_df: Store the rows which have errors
    :param column_name: Specify the column name
    :param values: Specify the data type of the column
    :param report_df: Store the status of each column in a dataframe
    """
    try:
        # Create a new row as a dictionary
        column_data_type = str(df_copy[column_name].dtype)
        column_match_status = False
        for value in values:
            if value in column_data_type:
                column_match_status = True
        if not column_match_status:
            report_df.loc[report_df['column'] == column_name, 'data_type'] = 1
            error_df = df_copy
            df_copy = pd.DataFrame(columns=df_copy.columns)
        return df_copy, error_df, report_df
    except Exception as e:
        print(f"Error occurred while validating data type : {e}")
        raise


def null_check_validation(df_copy, error_df, column_name, values, report_df):
    """
    The null_check_validation function takes in a DataFrame, an error DataFrame, the name of a column to check for null
    values, a list of values that should be considered null (e.g., [&quot;&quot;, &quot;nan&quot;]), and a report
    DataFrame. It then creates a boolean mask to identify rows with blank or NaN values in the specified column and
    filters the original dataframe to keep only rows without blank or NaN values in that column. The function returns
    both the filtered dataframe and an updated error dataframe containing the rows with blank or NaN values.

    :param df_copy: Pass the dataframe to be validated
    :param error_df: Store the rows with errors in a separate dataframe
    :param column_name: Specify the column to be validated
    :param values: Specify the values to be checked for null check validation
    :param report_df: Store the number of rows with blank or nan values in the specified column
    """
    try:
        if values == ["", "nan"]:
            # Create a boolean mask to identify rows with blank or NaN values in the specified column
            mask = df_copy[column_name].isna() | df_copy[column_name].eq('') | df_copy[column_name].eq(' ')
        elif values == [""]:
            mask = df_copy[column_name].eq('') | df_copy[column_name].eq(' ')
        elif values == ["nan"]:
            mask = df_copy[column_name].isna()
        # Filter the DataFrame to keep rows without blank or NaN values in the specified column
        valid_df = df_copy[~mask]
        # Create a separate DataFrame containing the error rows with blank or NaN values
        error_temp_df = df_copy[mask]
        error_df = error_df.append(error_temp_df, ignore_index=True)
        df_copy = valid_df
        report_df.loc[report_df['column'] == column_name, 'null_check'] = error_temp_df.shape[0]
        return df_copy, error_df, report_df
    except Exception as e:
        print(f"Error occurred while validating null check : {e}")
        raise


def is_date_format_matching(date_string, formats):
    """
    The is_date_format_matching function takes a date string and a list of formats.
        It then tries to match the date string with each format in the list. If it matches, it returns True, else False.

    :param date_string: Pass the date string to be validated
    :param formats: Provide the list of formats to be matched against
    :return: A boolean value
    """
    try:
        for date_format in formats:
            try:
                datetime.strptime(date_string, date_format)
                return True
            except ValueError:
                pass
        return False
    except Exception as e:
        print(f"Error occurred while matching date format : {e}")
        raise


def date_format_validation(df_copy, error_df, column_name, values, report_df):
    """
    The date_format_validation function takes in a dataframe, an error dataframe, the column name of the date field to
    be validated, a list of acceptable date formats and a report dataframe. It then filters out rows that do not match
    any of the specified date formats and appends them to an error_df. The number of rows filtered out is also added to
    the report_df.

    :param df_copy: Pass the dataframe to be validated
    :param error_df: Store the rows that do not match the date format
    :param column_name: Specify the column name for which the date format validation is to be performed
    :param values: Pass the list of date formats that are to be checked against
    :param report_df: Store the number of rows that have been filtered out
    """
    try:
        # Filter the DataFrame based on the desired date formats
        matching_rows = df_copy[df_copy[column_name].apply(lambda x: is_date_format_matching(str(x), values))]
        non_matching_rows = df_copy[~df_copy[column_name].apply(lambda x: is_date_format_matching(str(x), values))]

        df_copy = matching_rows
        error_df = error_df.append(non_matching_rows, ignore_index=True)
        report_df.loc[report_df['column'] == column_name, 'date_format'] = non_matching_rows.shape[0]
        return df_copy, error_df, report_df
    except Exception as e:
        print(f"Error while validating date format : {e}")
        raise


def upload_df_to_gcs(df, gcs_path, reference_file_name, file_name):
    """
    The upload_df_to_gcs function uploads a pandas dataframe to GCS.

    :param df: Pass the dataframe to be uploaded
    :param gcs_path: Specify the path to the gcs bucket
    :param reference_file_name: Create a folder in the gcs bucket to store the files
    :param file_name: Create the destination path in gcs
    """
    try:
        df_local_path = file_utils.save_df_to_temp_csv(df, "pandas")
        if "csv" in file_name:
            destination_path = f"{gcs_path.rstrip(r'/')}/{reference_file_name}/{file_name}"
        elif "gz" in file_name:
            destination_path = f"{gcs_path.rstrip(r'/')}/{reference_file_name}/{file_name.rstrip('gz')}csv"
        gcs_utils.upload_file_to_gcs(df_local_path, destination_path)
    except Exception as e:
        print(f"Error occurred while uploading df to gcs : {e}")
        raise


def validate(df, json_data, report_columns, report_path, error_path, reference_file_name, file_name):
    """
    The validate function takes in a dataframe, json_data, report_columns, report_path and error path as input.
    It then creates an empty dataframe for errors and reports. It also creates a dictionary of validations columns
    and their values to be used later on in the function. The function then iterates through each column name from the
    json file and appends it to the reports dataframe along with 0's for all other columns except 'column'. It then
    iterates through each validation type within that column name from the json file and stores them into variables
    which are used later on in this function

    :param df: Pass the dataframe to be validated
    :param json_data: Get the column name and validations to be performed on that column
    :param report_columns: Create the report dataframe
    :param report_path: Upload the report dataframe to gcs
    :param error_path: Upload the error dataframe to gcs bucket
    :param reference_file_name: Create a folder in the gcs bucket
    :param file_name: Upload the file to gcs with the same name as that of the input file
    """
    try:
        df_copy = df.copy()
        error_df = pd.DataFrame(columns=df.columns)
        report_df = pd.DataFrame(columns=report_columns)
        validations_col_dict = {f"{report_col}_validation": [] for report_col in report_columns[1:]}
        for each_col_dict in json_data:
            column_name = each_col_dict["name"]
            print(column_name)
            row_dict = {element: 0 for element in report_columns}
            row_dict['column'] = column_name
            report_df = report_df.append(row_dict, ignore_index=True)
            for each_validation in each_col_dict["validations"]:
                validation_name = each_validation["name"]
                valid_values = each_validation["values"]
                valid_name = validation_name + '_validation'
                temp_val = validations_col_dict[valid_name]
                temp_val.append({'column': column_name, 'values': valid_values})
                validations_col_dict[valid_name] = temp_val

        for validation_key, validation_col_params in validations_col_dict.items():
            for validation_col_param in validation_col_params:
                validation_column_name = validation_col_param["column"]
                validation_values = validation_col_param["values"]
                df_copy, error_df, report_df = globals()[validation_key](df_copy, error_df, validation_column_name,
                                                                         validation_values, report_df)
        upload_df_to_gcs(error_df, error_path, reference_file_name, file_name)
        upload_df_to_gcs(report_df, report_path, reference_file_name, file_name)
        return error_df
    except Exception as e:
        print(f"Error occurred while validating : {e}")
        raise


def process_file(local_path, error_path, report_path, reference_file_name, json_data,
                 permitted_extensions, report_columns):
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
        extension = local_path.split('.')[-1]
        file_name = local_path.split(r'/')[-1]
        spark = spark_utils.create_spark_session()
        local_new_path = f"file://{os.getcwd()}/{local_path}"
        print(f"Processing file : {local_path}")
        if extension in permitted_extensions:
            if extension == "csv" or extension == "gz":
                pyspark_df = spark.read.csv(local_new_path, header=True, inferSchema=True)
                # Create a partial function with the additional parameters
                validate_partial = partial(validate, json_data=json_data, report_columns=report_columns,
                                           report_path=report_path, error_path=error_path,
                                           reference_file_name=reference_file_name, file_name=file_name)
                # Apply the validate_partial function using applyInPandas
                error_df = pyspark_df.groupby().applyInPandas(lambda pdf: validate_partial(pdf),
                                                              schema=StructType.fromJson(pyspark_df.schema.jsonValue()))
                file_utils.save_df_to_temp_csv(error_df, "pyspark")
    except Exception as e:
        print(f"Error occurred while processing file : {e}")
        raise


def process_files(input_path, error_path, report_path, files, json_data, permitted_extensions, report_columns):
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
        all_files = gcs_utils.get_files_from_gcs_bucket(input_path)
        print(f"All files for processing : {all_files}")
        downloaded_paths = [gcs_utils.download_from_gcs(file_url) for file_url in all_files]
        all_params = []
        for downloaded_path in downloaded_paths:
            file_name = downloaded_path.split("/")[-1]
            for reference_file_name in files:
                if reference_file_name in file_name:
                    all_params.append((downloaded_path, error_path, report_path, reference_file_name,
                                       json_data[reference_file_name]["columns"], permitted_extensions, report_columns))

        # Process files in parallel using concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Submit the processing of each file as a separate thread, passing additional parameters
            futures = [executor.submit(process_file, *params) for params in all_params]

            # Wait for all threads to complete
            concurrent.futures.wait(futures, return_when=concurrent.futures.ALL_COMPLETED)

            # Iterate over futures and check for any raised exceptions
            for future in futures:
                future.result()
    except Exception as e:
        print(f"Error occurred while processing files : {e}")
        raise
