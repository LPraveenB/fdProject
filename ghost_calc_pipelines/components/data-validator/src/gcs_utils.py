from google.cloud import storage
import tempfile
import os
import uuid
import json

logger = None


def create_client(creds_json_path: str = None):
    """
    The create_client function creates a storage client object.
        Args:
            creds_json_path (str): The path to the credentials json file.

    :param creds_json_path: str: Specify the path to the credentials json file
    :return: A client object
    """
    try:
        if creds_json_path:
            client = storage.Client.from_service_account_json(creds_json_path)
            print("Storage client created with credentials")
            return client
        else:
            client = storage.Client()
            print("Storage client created without Credentials")
            return client
    except Exception as e:
        print(f"Storage client not created : {e}")
        raise


def get_files_from_gcs_bucket(bucket_path):
    """
    The get_files_from_gcs_bucket function retrieves all the files from a GCS bucket.

    :param bucket_path: Specify the path to the bucket
    :return: A list of file paths
    """
    try:
        # Remove the 'gs://' prefix from the bucket path
        bucket_path = bucket_path.replace('gs://', '')

        if '/' in bucket_path:
            # Path has a prefix specified
            bucket_name = bucket_path.split('/')[0]
            prefix = '/'.join(bucket_path.split('/')[1:])
        else:
            # Path is just the bucket name
            bucket_name = bucket_path
            prefix = None

        # Create a client object
        client = create_client()

        # Get the bucket
        bucket = client.get_bucket(bucket_name)

        # List all the blobs in the bucket with the given prefix
        blobs = bucket.list_blobs(prefix=prefix)

        file_paths = []
        for blob in blobs:
            bucket_name = bucket.name
            blob_name = blob.name
            full_path = f"gs://{bucket_name}/{blob_name}"
            file_paths.append(full_path)

        return file_paths

    except Exception as e:
        print(f"Error while retrieving the files from bucket url : {e}")
        raise


def download_from_gcs(gcs_url):
    """
    The download_from_gcs function downloads a file from Google Cloud Storage to the local filesystem.

    :param gcs_url: Specify the gcs url of the file to be downloaded
    :return: A local file path
    """
    try:
        # Create a Google Cloud Storage client
        client = create_client()

        # Split the GCS URL into bucket and object names
        bucket_name, object_name = gcs_url.replace('gs://', '').split('/', 1)

        # Get the bucket and blob
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(object_name)

        temp_dir = f'temp/{str(uuid.uuid4())[:8]}'

        # Create the local file path in the temp dir
        local_path = os.path.join(temp_dir, object_name)

        # Create the local path if it does not exist
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        # Download the blob into the local file
        blob.download_to_filename(local_path)

        return local_path
    except Exception as e:
        print(f"Error while downloading the file from bucket url : {e}")
        raise


def upload_file_to_gcs(local_file_path, gcs_bucket_url):
    """
    The upload_file_to_gcs function uploads a file to Google Cloud Storage.

    :param local_file_path: Specify the path of the file to be uploaded
    :param gcs_bucket_url: Specify the gcs bucket url where the file will be uploaded
    """
    try:
        # Create a storage client
        client = storage.Client()

        # Get the bucket name and file path from the GCS bucket URL
        bucket_name, gcs_file_path = gcs_bucket_url.split('/', 2)[2].split('/', 1)

        # Get the bucket reference
        bucket = client.bucket(bucket_name)

        # Create a blob object with the GCS file path
        blob = bucket.blob(gcs_file_path)

        # Upload the file to GCS
        blob.upload_from_filename(local_file_path)

        print(f"File uploaded to GCS: {gcs_bucket_url}")
    except Exception as e:
        print(f"Error occurred while uploading file to gcs : {e}")
        raise


def read_json_from_gcs(uri):
    try:
        # Create a client
        client = storage.Client()

        # Get the bucket and blob from the GCS URI
        bucket_name = uri.split("//")[1].split("/")[0]
        blob_name = "/".join(uri.split("//")[1].split("/")[1:])

        # Get the bucket and blob from the GCS URI
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)

        # Download the JSON file as a string
        json_string = blob.download_as_text()

        # Parse the JSON string into a dictionary
        data = json.loads(json_string)

        return data
    except Exception as e:
        print(f"Error while reading json from gcs : {e}")
        raise


def copy_file(source_uri, destination_uri):
    try:
        # Instantiate a client
        client = storage.Client()

        # Get the source bucket and file name from the source URI
        source_bucket_name, source_blob_name = source_uri.split("/", 3)[2:4]

        # Get the destination bucket and file name from the destination URI
        destination_bucket_name, destination_blob_name = destination_uri.split("/", 3)[2:4]

        # Get the source bucket
        source_bucket = client.get_bucket(source_bucket_name)

        # Get the source blob
        source_blob = source_bucket.blob(source_blob_name)

        # Get the destination bucket
        destination_bucket = client.get_bucket(destination_bucket_name)

        # Copy the source blob to the destination bucket
        destination_blob = source_bucket.copy_blob(
            source_blob, destination_bucket, new_name=destination_blob_name
        )

        print(
            f"File {source_bucket_name}/{source_blob_name} copied to {destination_bucket_name}/{destination_blob_name}"
        )
    except Exception as e:
        print(f"Error occurred while copying file : {e}")
        raise


def delete_file_from_gcs(gcs_uri):
    # Extracting the bucket and file path from the GCS URI
    parts = gcs_uri.split("/")
    bucket_name = parts[2]
    file_path = "/".join(parts[3:])

    # Instantiating the GCS client
    storage_client = storage.Client()

    try:
        # Getting the bucket
        bucket = storage_client.bucket(bucket_name)

        # Getting the blob (file) to be deleted
        blob = bucket.blob(file_path)

        # Deleting the blob
        blob.delete()

        print(f"File '{gcs_uri}' deleted successfully.")
    except Exception as e:
        print(f"Error deleting file '{gcs_uri}': {e}")
