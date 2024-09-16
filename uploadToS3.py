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

def lambda_handler(event, context):
    # List of bucket names from environment variables
    bucket_names = os.getenv('BUCKET_NAMES', '').split(',')
    sns_topic_arn = os.getenv('SNS_TOPIC_ARN')
    max_time_interval = int(os.getenv('MAX_TIME_INTERVAL', '60'))  # Expected time interval (in minutes)
    lambda_time = datetime.utcnow().isoformat()
    log_prefix = "AWSLogs"
    log_bucket = "aws-cloudtrail-logs-dataevent"
    output_bucket = os.getenv('OUTPUT_BUCKET')  # Output bucket for CSV
    output_csv_key = f'file_metadata_{lambda_time}.csv'  # Key for the CSV file

    csv_data = [["Prefix", "Filename", "Uploader", "Datetime_file_landed", "Datetime_lambda_ran"]]

    for bucket_name in bucket_names:
        try:
            # List objects in the bucket
            response = s3_client.list_objects_v2(Bucket=bucket_name)
            
            # Check if there are any files in the bucket
            if 'Contents' not in response:
                send_alert(sns_topic_arn, f"No files found in bucket: {bucket_name}.")
                continue

            now = datetime.utcnow().replace(tzinfo=timezone.utc)
            recent_files_found = False

            for obj in response['Contents']:
                file_key = obj['Key']
                last_modified_time = obj['LastModified']

                # Check if the file was uploaded within the expected time interval
                if now - last_modified_time <= timedelta(minutes=max_time_interval):
                    recent_files_found = True
                    uploader=fetch_logs(log_bucket, log_prefix, file_key, bucket_name)
                    file_metadata = {
                        "Prefix": file_key.split('/')[0],
                        "Filename": file_key.split('/')[-1],
                        "Uploader": uploader, 
                        "Datetime_file_landed": last_modified_time.strftime('%Y-%m-%d %H:%M:%S'),
                        "Datetime_lambda_ran": lambda_time
                    }
                    send_metadata_notification(sns_topic_arn, file_metadata)
                    csv_data.append([file_metadata["Prefix"], file_metadata["Filename"], file_metadata["Uploader"], file_metadata["Datetime_file_landed"], file_metadata["Datetime_lambda_ran"]])

            if not recent_files_found:
                send_alert(sns_topic_arn, f"No recent files have been uploaded to bucket: {bucket_name}.")

        except ClientError as e:
            print(e)
            send_alert(sns_topic_arn, f"An error occurred in bucket {bucket_name}: {str(e)}")

    write_csv_to_s3(csv_data, output_bucket, output_csv_key)

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
    # Get list of logs from the logging bucket
    response = s3_client.list_objects_v2(Bucket=log_bucket, Prefix=log_prefix)
    logs = response.get('Contents', [])

    if not logs:
        print("No logs found in the specified bucket/prefix.")
        return

    for log in logs:
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
                            return user_identity.get('userName', 'Unknown'), user_identity.get('arn', 'Unknown'), record.get('eventTime')
                            # return user_identity.get('userName', 'Unknown')
            except json.JSONDecodeError:
                print(f"Skipping invalid JSON line in file: {log_key}")
                continue

    print(f"No PutObject entries found for the object: {file_key}")
