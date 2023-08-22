gcs_uri = "gs://user-bucket-dollar-tree/vinaykumar.p/data_validator/20230712_121312/success/FD_RETURNS/FD_RETURNS-01072023-1231122.csv"
# print(local_path.split('/'))
# destination_path = ('/'.join(local_path.split('/')[-2:])).split(".")[0]
destination_path = "/".join(gcs_uri.split("/")[-2:])
print(destination_path)
