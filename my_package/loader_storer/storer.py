import csv
from pathlib import Path


def store_csv(path: Path, data):
    # Write list of dictionaries to CSV file
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        for d in data:
            writer.writerow(d)


def store_csv_s3(key, bucket_name, data):
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    import tempfile

    s3_hook = S3Hook(aws_conn_id="minio_conn")
    temp_file = tempfile.NamedTemporaryFile()
    with open(temp_file.name, "w") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        for d in data:
            writer.writerow(d)
    print(dict(filename=temp_file.name, key=key, bucket_name=bucket_name))
    s3_hook.load_file(filename=temp_file.name, key=key, bucket_name=bucket_name)
