import json
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
import os

# Initialize clients for S3 and SNS
s3_client = boto3.client('s3')
sns_client = boto3.client('sns')

def lambda_handler(event, context):
    # List of bucket names from environment variables
    bucket_names = os.getenv('BUCKET_NAMES', '').split(',')
    sns_topic_arn = os.getenv('SNS_TOPIC_ARN')
    max_time_interval = int(os.getenv('MAX_TIME_INTERVAL', '60'))  # Expected time interval (in minutes)
    lambda_time = datetime.utcnow().isoformat()

    for bucket_name in bucket_names:
        try:
            # List objects in the bucket
            response = s3_client.list_objects_v2(Bucket=bucket_name)
            
            # Check if there are any files in the bucket
            if 'Contents' not in response:
                send_alert(sns_topic_arn, f"No files found in bucket: {bucket_name}.")
                continue

            now = datetime.utcnow()
            recent_files_found = False

            for obj in response['Contents']:
                file_key = obj['Key']
                last_modified_time = obj['LastModified']

                # Check if the file was uploaded within the expected time interval
                if now - last_modified_time <= timedelta(minutes=max_time_interval):
                    recent_files_found = True
                    file_metadata = {
                        "Prefix": file_key.split('/')[0],
                        "Filename": file_key.split('/')[-1],
                        "Uploader": "unknown",  # You may need to fetch or determine the uploader differently
                        "Datetime_file_landed": last_modified_time.strftime('%Y-%m-%d %H:%M:%S'),
                        "Datetime_lambda_ran": now.strftime('%Y-%m-%d %H:%M:%S')
                    }
                    send_metadata_notification(sns_topic_arn, file_metadata)

            if not recent_files_found:
                send_alert(sns_topic_arn, f"No recent files have been uploaded to bucket: {bucket_name}.")

        except ClientError as e:
            print(e)
            send_alert(sns_topic_arn, f"An error occurred in bucket {bucket_name}: {str(e)}")

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
