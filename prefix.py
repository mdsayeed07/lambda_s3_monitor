import json
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timedelta, timezone
import os
import gzip
import io
import csv

# Initialize clients for S3 and SNS
s3_client = boto3.client('s3')
sns_client = boto3.client('sns')


year = datetime.now().year
month = datetime.now().strftime('%m')
day = datetime.now().strftime('%d')
log_bucket = "aws-cloudtrail-logs-dataevent"
log_prefix = f"AWSLogs/211125347349/CloudTrail/us-east-1/{year}/{month}"  # customize logs prefix to scan only today's logs

bucket_names = os.getenv('BUCKET_NAMES', 'athena-glue-1205:csv/logs/').split(',')
lambda_time = datetime.utcnow().strftime('%Y-%m-%d_%H:%M:%S')
max_time_interval = int(os.getenv('MAX_TIME_INTERVAL', '3'))

output_bucket = os.getenv('OUTPUT_BUCKET', 'rtlab-petclinic-logstore-s3')
output_csv_key = f'csv/nfl/logs/{lambda_time}_file_metadata.csv'
sns_topic_arn = os.getenv('SNS_TOPIC_ARN')

csv_data = [["Bucket_name", "Prefix", "Filename", "Uploader", "Datetime_file_landed", "Datetime_lambda_ran"]]

def write_csv_to_s3(csv_data, output_bucket, output_csv_key):
    try:
        csv_buffer = io.StringIO()
        writer = csv.writer(csv_buffer)
        writer.writerows(csv_data)
        
        s3_client.put_object(Bucket=output_bucket, Key=output_csv_key, Body=csv_buffer.getvalue())
        print(f"CSV file uploaded to {output_bucket}/{output_csv_key}")
    except ClientError as e:
        print(f"Error uploading CSV to S3: {e}")

# Send alert if no file or an issue arises
def send_alert(sns_topic_arn, message):
    try:
        sns_client.publish(
            TopicArn=sns_topic_arn,
            Message=message,
            Subject="S3 File Notification Alert"
        )
        print("Alert sent successfully")
    except ClientError as e:
        print(f"Error sending alert: {e}")

# Send metadata about the recent file
def send_metadata_notification(sns_topic_arn, metadata):
    try:
        sns_client.publish(
            TopicArn=sns_topic_arn,
            Message=json.dumps(metadata),
            Subject="S3 File Metadata Notification"
        )
        print("Metadata notification sent successfully")
    except ClientError as e:
        print(f"Error sending metadata notification: {e}")

def fetch_logs(log_bucket, log_prefix, file_key, bucket_name):
    # Get the current time in UTC
    now = datetime.utcnow().replace(tzinfo=timezone.utc)
    
    # Get the list of objects/logs from the S3 bucket
    response = s3_client.list_objects_v2(Bucket=log_bucket, Prefix=log_prefix)
    
    # Retrieve the contents (logs) from the response
    logs = response.get('Contents', [])
    
    # If no logs are found, exit
    if not logs:
        print("No logs found in the specified bucket/prefix.")
        return

    # Iterate through each log and filter based on the LastModified time
    for log in logs:
        last_modified_time_logs = log['LastModified']

        # Check if the log was modified within the last 'time_interval_minutes' (e.g., 15 minutes)
        if now - last_modified_time_logs <= timedelta(minutes=150):
            log_key = log['Key']
            log_obj = s3_client.get_object(Bucket=log_bucket, Key=log_key)
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
                            if request_params.get('bucketName') == bucket_name and request_params.get('key') == file_key:
                                user_identity = record.get('userIdentity', {})
                                print(f"Event: {record.get('eventName')} at {record.get('eventTime')}")
                                print(f"File {file_key} was uploaded by User: {user_identity.get('userName', 'Unknown')}, ARN: {user_identity.get('arn', 'Unknown')}")
                                userName = user_identity.get('arn').split('/')[-1]
                                print(f"UserName : {userName}")
                                return user_identity.get('arn', 'Unknown')
                                # return user_identity.get('userName', 'Unknown')
                except json.JSONDecodeError:
                    print(f"Skipping invalid JSON line in file: {log_key}")
                    continue

    print(f"No PutObject entries found for the object: {file_key} - {bucket_name}")

# athena-glue-1205:csv/logs/,rtlab-petclinic-logstore-s3:csv/nfl/logs/



for nfl_bucket in bucket_names:
    bucket_name, prefix = nfl_bucket.split(':')
    try:
        # List objects in the bucket
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        
        # Check if there are any files in the bucket
        if 'Contents' not in response:
            # send_alert(sns_topic_arn, f"No files found in bucket: {bucket_name} with {prefix}.")
            print(f"No files found in bucket: {bucket_name} with {prefix}.")
            continue

        now = datetime.utcnow().replace(tzinfo=timezone.utc)
        recent_files_found = False

        for obj in response['Contents']:
            file_key = obj['Key']
            last_modified_time = obj['LastModified']

            if file_key == prefix:
                continue
            
            print(file_key)

            # Check if the file was uploaded within the expected time interval
            if now - last_modified_time <= timedelta(hours=max_time_interval):
                recent_files_found = True
                uploader=fetch_logs(log_bucket, log_prefix, file_key, bucket_name)
                file_metadata = {
                    "Prefix": prefix,
                    "Filename": file_key.split('/')[-1],
                    "Uploader": uploader, 
                    "Datetime_file_landed": last_modified_time.strftime('%Y-%m-%d_%H:%M:%S'),
                    "Datetime_lambda_ran": lambda_time
                }

                # send_metadata_notification(sns_topic_arn, file_metadata)
                print(file_metadata)
                csv_data.append([bucket_name, file_metadata["Prefix"], file_metadata["Filename"], file_metadata["Uploader"], file_metadata["Datetime_file_landed"], file_metadata["Datetime_lambda_ran"]])

        if not recent_files_found:
            # send_alert(sns_topic_arn, f"No recent files have been uploaded to bucket: {bucket_name} with {prefix}.")
            print(f"No recent files have been uploaded to bucket: {bucket_name} with {prefix}.")

    except ClientError as e:
        print(e)
        # send_alert(sns_topic_arn, f"An error occurred in bucket {bucket_name} with {prefix}: {str(e)}")
        print(f"An error occurred in bucket {bucket_name} with {prefix}: {str(e)}")

write_csv_to_s3(csv_data, output_bucket, output_csv_key)

