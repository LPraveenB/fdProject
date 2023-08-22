import json
from urllib.parse import urlparse
from google.cloud import storage
from pyspark.sql import SparkSession

FILENAME_DELIMITER = '-'
DF_DATE_FORMAT = '%Y-%m-%d'
FILENAME_UPDATED_FORMAT = '%Y%m%d%H%M%S'

def list_files(files_path):
    """
    :param files_path: GCS Path
    :return: list of files(with full gcs path) under the given gcs path
    """
    gcs_path = urlparse(files_path, allow_fragments=False)
    bucket_name, filename = gcs_path.netloc, gcs_path.path.lstrip("/")
    print('bucket_name',  bucket_name)
    print('filename', filename)
    client = storage.Client()
    return list(client.list_blobs(bucket_name, prefix=filename))

def get_file_extension(full_name):
    """
    :param full_name: File name with extension (e.g., FD_STORE_INVENTORY-2023-04-01-1.csv)
    :return: the File name part and extension part seperated out (e.g., FD_STORE_INVENTORY-2023-04-01-1 and .csv)
    """
    file_name = None
    extension = ''
    name_split = full_name.split('.')
    i = 0
    for each_split in name_split:
        if i == 0:
            file_name = each_split
        else:
            extension = extension  + '.' + each_split
        i =  i +  1
    return file_name,  extension

def convert_to_dict(src_path):
    """
    Builds a dictionary with list of full gcs path files mapped to table name. e.g.,  FD_STORE_INV -> ['FD_STORE_INV-1.csv','FD_STORE_INV-2.csv']
    :param src_path: GCS Path to consider
    :return: the  dictionary
    """
    files_dict = {}
    drop_files = list_files(src_path)
    for each_blob in drop_files:
        gcs_file_name = each_blob.name.split('/')[-1]
        if gcs_file_name.strip() != '':
            file_name, file_ext = get_file_extension(gcs_file_name)
            if file_ext == '.csv' or file_ext  =='.csv.gz':
                file_name_split = file_name.split(FILENAME_DELIMITER)
                if len(file_name_split) > 1:
                    obj_name = file_name_split[0]

                    if obj_name in files_dict:
                        file_list = files_dict[obj_name]
                    else:
                        file_list = []

                    file_list.append('gs://' + each_blob.bucket.name + '/' + each_blob.name)
                    files_dict[obj_name] = file_list
    return files_dict


def get_spark_session():
    app_name = "Spark Application"
    master = """local[*]"""
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    return spark

def blob_exists(file_path):
    """
    :param file_path: GCS Path
    :return: Boolean indicating whether the given path exists or not.
    """
    gcs_path = urlparse(file_path, allow_fragments=False)
    bucket_name, filename = gcs_path.netloc, gcs_path.path.lstrip("/")
    client = storage.Client()
    return len(list(client.list_blobs(bucket_name, prefix=filename))) > 0

def get_full_path(src_path, table_name, location_group_id):
    return src_path + '/' + table_name + '.parquet/LOCATION_GROUP=' + str(location_group_id) + '/'

def move_files(src_path, dest_path):
    """
    Helper method to move all files under given Source GCS Path to given Destination GCS Path
    Suffix the source files created/modified timestamp to destination file name to avoid overwriting the same file name dropped multiple times in source location.
    :param src_path: Source GCS Path
    :param dest_path: Destination GCS Path
    """
    src_gcs_path = urlparse(src_path, allow_fragments=False)
    src_bucket_name, src_file = src_gcs_path.netloc, src_gcs_path.path.lstrip("/")
    src_file_name = src_file.split('/')[-1]
    file_name, file_ext = get_file_extension(src_file_name)
    dest_gcs_path = urlparse(dest_path, allow_fragments=False)
    dest_bucket_name, dest_folder = dest_gcs_path.netloc, dest_gcs_path.path.lstrip("/")

    client = storage.Client()
    src_bucket = storage.Bucket(client, src_bucket_name)
    dest_bucket = storage.Bucket(client, dest_bucket_name)
    src_blob = src_bucket.blob(src_file)
    src_blob.reload()
    if not dest_folder.endswith('/'):
        dest_folder  = dest_folder + '/'
    dest_file = dest_folder + file_name + FILENAME_DELIMITER + src_blob.updated.strftime(
        FILENAME_UPDATED_FORMAT) + file_ext
    src_bucket.copy_blob(src_blob, dest_bucket, dest_file)
    #src_blob.delete() # Temporarily commented for testing

