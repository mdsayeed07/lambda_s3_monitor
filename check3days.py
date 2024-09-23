import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timedelta, timezone
import json
import gzip
import csv
import io
import os
 
# Initialize clients for S3 and SNS
s3_client = boto3.client('s3')
sns_client = boto3.client('sns')
 
# CloudTrail related variables
year = datetime.now().year
month = datetime.now().strftime('%m')
day = int(datetime.now().strftime('%d'))
log_bucket = os.getenv('LOG_BUCKET', 'aws-cloudtrail-logs-dataevent')
log_prefix = f"AWSLogs/211125347349/CloudTrail/us-east-1/{year}/{month}/"  # customize logs prefix to scan only today's logs
 
# bucket_names = bucket01:prefix01,bucket02:prefix....
bucket_names = os.getenv('BUCKET_NAMES', 'athena-glue-1205:csv/logs/').split(',')
lambda_time_ran = datetime.utcnow().strftime('%Y-%m-%d_%H:%M:%S')
max_time_interval = int(os.getenv('MAX_TIME_INTERVAL', '3')) # default is 3 hours
 
# Bucket to store csv output
# output_bucket = os.getenv('OUTPUT_BUCKET')
# output_csv_key = f'nfl/csv/{lambda_time_ran}_file_metadata.csv'
# sns_topic_arn = os.getenv('SNS_TOPIC_ARN')
 
output_bucket = os.getenv('OUTPUT_BUCKET', 'rtlab-petclinic-logstore-s3')
output_csv_key = f'csv/nfl/logs/{lambda_time_ran}_file_metadata.csv'
sns_topic_arn = os.getenv('SNS_TOPIC_ARN', 'arn:aws:sns:us-east-1:211125347349:lambda-py')

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
 
def send_notification(sns_topic_arn, body):
    try:
        sns_client.publish(
            TopicArn=sns_topic_arn,
            Message=body,
            Subject="NFL S3 File Metadata Notification"
        )
        print("Metadata notification sent successfully")
    except ClientError as e:
        print(f"Error sending metadata notification: {e}")
 
def fetch_logs(log_bucket, log_prefix, file_key, bucket_name, current_time_utc):
 
    # log_prefixs=[log_prefix + str(day), log_prefix + str(day - 1), log_prefix + str(day + 1)]
    days_offset = [0, -1, 1]
    log_prefixes = []

    for offset in days_offset:
        log_date = current_time_utc + timedelta(days=offset)
        year = log_date.strftime('%Y')
        month = log_date.strftime('%m')
        day = log_date.strftime('%d').lstrip('0')  # Remove leading zeros
        log_prefix = f"AWSLogs/211125347349/CloudTrail/us-east-1/{year}/{month}/{day}/"
        log_prefixes.append(log_prefix)

    for log_date in log_prefixes:
        user_identity = None
        print(log_date)
        # Get the list of objects/logs from the CloudTrail S3 bucket
        response = s3_client.list_objects_v2(Bucket=log_bucket, Prefix=log_date)
    
        # Retrieve the contents (logs) from the response
        logs = response.get('Contents', [])
    
        if not logs:
            print("No logs found in the specified bucket/prefix.")
            return
    
        # Iterate through each log and filter based on the LastModified time to scan only latest logs
        for log in logs:
            last_modified_time_logs = log['LastModified']
    
            # Check and continue if the log was modified within the 'max_time_interval'
            if current_time_utc - last_modified_time_logs <= timedelta(hours=max_time_interval):
                log_key = log['Key']
                log_obj = s3_client.get_object(Bucket=log_bucket, Key=log_key)
                log_content = log_obj['Body'].read()
    
                times = log['LastModified']
                print(times)
    
                # Decompress gzipped formatted logs 
                with gzip.GzipFile(fileobj=io.BytesIO(log_content)) as log_file:
                    log_lines = [line.decode('utf-8') for line in log_file]
    
                for line in log_lines:
                    try:
                        event_data = json.loads(line)
                        for record in event_data.get('Records', []):
                            # Proceed if record is for PutObject 
                            if record.get('eventName') == 'PutObject':
                                request_params = record.get('requestParameters', {})
                                # Check if the log is matches with NFL Bucket Object
                                if request_params.get('bucketName') == bucket_name and request_params.get('key') == file_key:
                                    user_identity = record.get('userIdentity', {})
                                    return user_identity.get('arn', 'Unknown').split('/')[-1]
    
                    except json.JSONDecodeError as e:
                        print(f"Skipping invalid JSON line in file: {log_key} : error : {e}")
                        continue

        # If we've found a username, break the loop
        if user_identity:
            break
                    
    print(f"No PutObject entries found in logs for the object: {bucket_name}/{file_key} abborting ...")
 
def main():
    for nfl_bucket in bucket_names:
        bucket_name, prefix = nfl_bucket.split(':')
        try:
            # List objects in the bucket as per given prefix
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
 
            if 'Contents' not in response:
                send_notification(sns_topic_arn, body=f"No files found in bucket: {bucket_name} with prefix: {prefix}.")
                continue
 
            current_time_utc = datetime.utcnow().replace(tzinfo=timezone.utc)
 
            # Flag to determine if any file have been modified in given time interval
            recent_files_found = False
 
            for obj in response['Contents']:
                # absolute path (full path) of a selected file
                file_key = obj['Key']
                last_modified_time = obj['LastModified']
                key_prefix = os.path.dirname(file_key)
                filename = os.path.basename(file_key)
 
                # skip if response returns empty folder as object
                if not filename:
                    continue  
 
                # Check if the file was uploaded within the given time interval
                if current_time_utc - last_modified_time <= timedelta(hours=max_time_interval):
 
                    # recent_files_found defaults to False, if any file is modified it will return
                    # true outside of loop
                    recent_files_found = True
 
                    # Get file/object uploader name
                    uploader=fetch_logs(log_bucket, log_prefix, file_key, bucket_name, current_time_utc)
 
                    # Skipping because logs are not generated and uploader is empty
                    if uploader is None:
                        continue
 
                    file_metadata = {
                        "Prefix": key_prefix,
                        "Filename": filename,
                        "Uploader": uploader, 
                        "Datetime_file_landed": last_modified_time.strftime('%Y-%m-%d_%H:%M:%S'),
                        "Datetime_lambda_ran": lambda_time_ran
                    }
                    send_notification(sns_topic_arn, body=f"NFL S3 file processing using Lambda Function for the bucket {bucket_name} \n\nMetadata: \n{file_metadata}")
                    csv_data.append([bucket_name, file_metadata["Prefix"], file_metadata["Filename"], file_metadata["Uploader"], file_metadata["Datetime_file_landed"], file_metadata["Datetime_lambda_ran"]])
 
            if not recent_files_found:
                send_notification(sns_topic_arn, body=f"No recent files have been uploaded to bucket: {bucket_name}/{prefix}.")
 
        except ClientError as e:
            print(e)
            send_notification(sns_topic_arn, body=f"An error occurred while processing bucket: {bucket_name}/{prefix} \n\nError: {str(e)}")
 
# def lambda_handler(event, context):
main()
write_csv_to_s3(csv_data, output_bucket, output_csv_key)

