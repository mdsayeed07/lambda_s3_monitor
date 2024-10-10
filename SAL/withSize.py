import boto3
import csv
import io
import re
import os
from botocore.exceptions import ClientError
from datetime import datetime, timezone, timedelta
 
###
## Generalized regex to match both IAM and AD user ARNs
## user_pattern = re.compile(r'(arn:aws:iam::\d+:user/[^\s]+|arn:aws:iam::\d+:role/[^\s]+|[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)')
### 

# Initialize clients for S3 and SNS
s3_client = boto3.client('s3')
sns_client = boto3.client('sns')
 
# Server Access Logs related variables
# log_bucket = 'aws-cloudtrail-logs-122036648197-3b304768-nfl-s3-monitor'
# log_prefix = "s3/nfl/"
log_bucket = 'aws-logs-useast01'
log_prefix = "Awslogs/s3/"
 
# bucket_names format = bucket01:prefix01,bucket02:prefix....
bucket_names = os.getenv('BUCKET_NAMES').split(',')
lambda_time_ran = datetime.utcnow().strftime('%Y-%m-%d_%H:%M:%S')

# Bucket to store csv output
output_bucket = os.getenv('OUTPUT_BUCKET', 'rtlab-petclinic-logstore-s3')
output_csv_key = f'csv/nfl/log/{lambda_time_ran}_file_metadata.csv'
sns_topic_arn = os.getenv('SNS_TOPIC_ARN', 'arn:aws:sns:us-east-1:211125347349:lambda-py')

max_time_interval = 50
current_time_utc = datetime.utcnow().replace(tzinfo=timezone.utc)

file_metadatas = []
csv_data = [["Bucket_name", "Prefix", "Filename", "Uploader", "Datetime_file_landed", "Datetime_lambda_ran", "Error_if_any"]]
 
def write_csv_to_s3():
    try:
        csv_buffer = io.StringIO()
        writer = csv.writer(csv_buffer)
        writer.writerows(csv_data)
 
        s3_client.put_object(Bucket=output_bucket, Key=output_csv_key, Body=csv_buffer.getvalue())
        print(f"CSV file uploaded to {output_bucket}/{output_csv_key}")
 
    except ClientError as e:
        print(f"Error uploading CSV to S3: {e}")
 
def send_notification(body):
    try:
        sns_client.publish(
            TopicArn=sns_topic_arn,
            Message=body,
            Subject="NFL S3 File Metadata Notification"
        )
        print("Metadata notification sent successfully")

    except ClientError as e:
        print(f"Error sending metadata notification: {e}")

def list_all_objects(log_bucket, log_prefix):
    # list to append logs/objects
    log_objects = []
    continuation_token = None

    try:
        # while loop which will iterate based continuation_token
        while True:
            if continuation_token:
                response = s3_client.list_objects_v2(
                    Bucket=log_bucket,
                    Prefix=log_prefix,
                    ContinuationToken=continuation_token
                )
            else:
                response = s3_client.list_objects_v2(
                    Bucket=log_bucket,
                    Prefix=log_prefix
                )
    
            for obj in response.get('Contents', []):
                log_key = obj['Key']
                log_last_modified = obj['LastModified'] 
    
                # Check if the log file was modified within the expected time interval
                if current_time_utc - log_last_modified > timedelta(hours=30):
                    continue
    
                log_objects.append(log_key)
    
            # outside of for loop
            if response.get('IsTruncated'):
                continuation_token = response['NextContinuationToken']
            else:
                break
    
        return log_objects
    
    except ClientError as e:
        print(e)
 
def fetch_uploader(nfl_file_key, nfl_bucket_name, log_objects):
    try:
        if not log_objects:
            print(f"No logs found in the bucket {log_bucket}/{log_prefix}.")
            return None
 
        put_pattern = re.compile(r'REST.PUT.OBJECT')
 
        # Iterate through each log and filter based on the LastModified time to scan only latest logs
        for log_object in log_objects:
 
            log_data = s3_client.get_object(Bucket=log_bucket, Key=log_object)['Body'].read()
            log_str = log_data.decode('utf-8')
 
            # Check for specific bucket name and object key
            if put_pattern.search(log_str) and nfl_bucket_name in log_str and nfl_file_key in log_str:       
            # if put_pattern.search(log_str):
                match = re.search(r'(arn:aws:iam::\d+:user/[^\s]+)', log_str)
                if match:
                    username_arn = match.group(1)
                    print(f"Found PUT object: {username_arn}")
                    return username_arn.split('/')[-1]
                else:
                    print("Found Put object, but arn not available")
 
    except ClientError as e:
        print(e)
 
def main():
    try:
        # Fetch logs from the S3 bucket which is modified within the expected time interval
        log_objects = list_all_objects(log_bucket, log_prefix)

        # Loop NFL Buckets
        for nfl_bucket in bucket_names:
            nfl_bucket_name, nfl_bucket_prefix = nfl_bucket.split(':')        
            # List objects in the bucket as per given prefix
            response = s3_client.list_objects_v2(Bucket=nfl_bucket_name, Prefix=nfl_bucket_prefix)

            if 'Contents' not in response:
                send_notification(body=f"No files found in bucket: {nfl_bucket_name} with prefix: {nfl_bucket_prefix}.")
                continue

            # Flag to determine if any file have been modified in given time interval
            recent_files_found = False

            for obj in response['Contents']:
                # absolute path (full path) of a selected file
                nfl_file_key = obj['Key']
                nfl_last_modified_time = obj['LastModified']
                nfl_file_size = f"{obj['Size']/1024:.2f} KB"
                nfl_key_prefix = os.path.dirname(nfl_file_key)
                nfl_filename = os.path.basename(nfl_file_key)

                # skip if response returns empty folder as object
                if not nfl_filename:
                    continue

                # Check if the file was uploaded within the given time interval
                if current_time_utc - nfl_last_modified_time <= timedelta(hours=max_time_interval):
                    # recent_files_found defaults to False, if any file is modified it will return
                    # true outside of loop
                    recent_files_found = True

                    # Get file/object uploader name
                    uploader_name = fetch_uploader(nfl_file_key, nfl_bucket_name, log_objects)

                    # Skipping because logs are not generated and uploader is empty
                    if uploader_name is None:
                        uploader_name = "Logs not uploaded yet"

                    file_metadata = {
                        "Bucket": nfl_bucket_name,
                        "Prefix": nfl_key_prefix,
                        "Filename": nfl_filename,
                        "Uploader": uploader_name,
                        "File_size": nfl_file_size, 
                        "Datetime_file_landed": nfl_last_modified_time.strftime('%Y-%m-%d_%H:%M:%S'),
                        "Datetime_lambda_ran": lambda_time_ran
                    }
                    csv_data.append([nfl_bucket_name, file_metadata["Prefix"], file_metadata["Filename"], file_metadata["Uploader"], file_metadata["File_size"], file_metadata["Datetime_file_landed"], file_metadata["Datetime_lambda_ran"], "NoErrors"])
                    file_metadatas.append(file_metadata)

            if not recent_files_found:
                send_notification(body=f"No recent files have been uploaded to bucket: {nfl_bucket_name}/{nfl_bucket_prefix}.")
  
    except ClientError as e:
        error = str(e)
        csv_data.append([nfl_bucket_name, nfl_bucket_name, nfl_filename, uploader_name, nfl_last_modified_time.strftime('%Y-%m-%d_%H:%M:%S'), lambda_time_ran, error])
        print(e)
        send_notification(body=f"An error occurred while processing bucket: {nfl_bucket_name}/{nfl_bucket_prefix} \n\nError: {str(e)}")

def lambda_handler(event, context):
    main()
    write_csv_to_s3()
    send_notification(body=f"NFL S3 file processing using Lambda Function : \n\nMetadata: \n\n{file_metadatas}")
