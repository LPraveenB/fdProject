from google.cloud import storage


def check_gcs_uri_exists(uri):
    try:
        storage_client = storage.Client()
        bucket_name, object_path = parse_gcs_uri(uri)
        print(f"bucket_name {bucket_name}")
        print(f"object_path {object_path}")
        bucket = storage_client.bucket(bucket_name)
        print(f"bucket {bucket}")
        blob = bucket.blob(object_path)
        print(f"blob {blob}")
        exists = False
        max_retries = 5
        retry_delay = 1  # seconds
        for _ in range(max_retries):
            if blob.exists():
                exists = True
            time.sleep(retry_delay)
        print(f"exists {exists}")
        if exists:
            print("uri exists " + uri)
        else:
            print("uri not exists " + uri)
        return exists
    except Exception as e:
        print(f"An error occurred: {e}")
        return False



def list_gcs_folders(uri):
    bucket_name = uri.split('/')[2]
    print(f"bucket_name {bucket_name}")
    prefix = '/'.join(uri.split('/')[3:])
    print(f"prefix {prefix}")

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix, delimiter='/')
    print(f"blobs {list(blobs)}")
    folder_names = []
    for prefix in blobs.prefixes:
        folder_names.append(f"gs://{bucket_name}/{prefix}")
    return folder_names
