import tempfile
import os
import uuid
import shutil
import json

logger = None


def save_df_to_temp_csv(dataframe, file_type):
    """
    The save_df_to_temp_csv function saves a DataFrame to a temporary CSV file.

    :param dataframe: Pass in the dataframe to be saved
    :param file_type: Determine if the dataframe is a pyspark or pandas dataframe
    :return: A temporary csv file path
    """
    try:
        # Generate a temporary CSV file path
        temp_csv_path = f'temp/{str(uuid.uuid4())[:8]}/temp_file.csv'

        # Create the local path if it does not exist
        os.makedirs(os.path.dirname(temp_csv_path), exist_ok=True)
        if file_type == "pyspark":
            # Save DataFrame to the temporary CSV file
            dataframe.toPandas().to_csv(temp_csv_path, mode='w', index=False)
        elif file_type == "pandas":
            # Save DataFrame to the temporary CSV file
            dataframe.to_csv(temp_csv_path, mode='w', index=False)

        return temp_csv_path
    except Exception as e:
        print(f"Error occurred while saving df to temp folder : {e}")
        raise


def remove_directory(directory_path: str):
    """
    The remove_directory function removes a directory and all of its contents.

    :param directory_path: str: Specify the path of the directory to be removed
    :return: True if the directory was removed, false otherwise
    """
    try:
        if os.path.exists(directory_path):
            shutil.rmtree(directory_path)
            print(f"Directory '{directory_path}' removed successfully.")
        else:
            print(f"Directory '{directory_path}' does not exist.")
    except Exception as e:
        print(f"Error removing directory '{directory_path}': {e}")
        raise


def read_json_file(file_path):
    """
    The read_json_file function reads a json file and returns the data in it.
        Args:
            file_path (str): The path to the json file.

    :param file_path: Specify the path of the file to be read
    :return: A dictionary
    """
    try:
        with open(file_path) as f:
            json_data = json.load(f)
        return json_data
    except Exception as e:
        print(f"Error occurred while reading json file : {e}")
        raise
