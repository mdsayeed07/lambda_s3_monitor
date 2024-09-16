import boto3
import gzip
import io
import json

s3 = boto3.client('s3')

def fetch_logs(log_bucket, log_prefix, object_key):
    # Get list of logs from the logging bucket
    response = s3.list_objects_v2(Bucket=log_bucket, Prefix=log_prefix)
    logs = response.get('Contents', [])

    if not logs:
        print("No logs found in the specified bucket/prefix.")
        return

    for log in logs:
        log_key = log['Key']
        log_obj = s3.get_object(Bucket=log_bucket, Key=log_key)
        log_content = log_obj['Body'].read()

        # Decompress if gzipped
        if log_content[:2] == b'\x1f\x8b':
            try:
                with gzip.GzipFile(fileobj=io.BytesIO(log_content)) as log_file:
                    log_lines = [line.decode('utf-8') for line in log_file]
            except gzip.BadGzipFile:
                print(f"Skipping corrupted gzipped file: {log_key}")
                continue
        else:
            log_lines = log_content.decode('utf-8').splitlines()

        for line in log_lines:
            try:
                event_data = json.loads(line)
                for record in event_data.get('Records', []):
                    if record.get('eventName') == 'PutObject':
                        request_params = record.get('requestParameters', {})
                        if request_params.get('bucketName') == bucket_want and request_params.get('key') == object_key:
                            user_identity = record.get('userIdentity', {})
                            print(f"Event: {record.get('eventName')} at {record.get('eventTime')}")
                            print(f"File {object_key} was uploaded by User: {user_identity.get('userName', 'Unknown')}, ARN: {user_identity.get('arn', 'Unknown')}")
                            return user_identity.get('userName', 'Unknown'), user_identity.get('arn', 'Unknown'), record.get('eventTime')
            except json.JSONDecodeError:
                print(f"Skipping invalid JSON line in file: {log_key}")
                continue

    print(f"No PutObject entries found for the object: {object_key}")

# Usage
bucket_want = "athena-glue-1205"
log_bucket = "aws-cloudtrail-logs-dataevent"
log_prefix = "AWSLogs"
object_key = "next.py"

fetch_logs(log_bucket, log_prefix, object_key)

