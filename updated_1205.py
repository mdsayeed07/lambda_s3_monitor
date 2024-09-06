import json
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
import os

# Initialize clients for S3 and SNS
s3_client = boto3.client('s3')
sns_client = boto3.client('sns')

def lambda_handler(event, context):
    # Define the bucket name and expected file prefix from environment variables
    bucket_name = os.getenv('BUCKET_NAME')
    sns_topic_arn = os.getenv('SNS_TOPIC_ARN')
    expected_file_prefix = os.getenv('FILE_PREFIX')
    uploader = os.getenv('UPLOADER')  # Get the uploader (SSM or environment variable)
    max_time_interval = int(os.getenv('MAX_TIME_INTERVAL', '60'))  # Expected time interval (in minutes)

    try:
        # List objects in the bucket with the specified prefix
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=expected_file_prefix)
        
        # Check if there are any files in the bucket
        if 'Contents' not in response:
            return send_alert(sns_topic_arn, "No files found in the bucket.")

        # Get the most recent file based on LastModified timestamp
        files = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)
        recent_file = files[0]  # The most recent file
        recent_file_time = recent_file['LastModified']
        now = datetime.utcnow()

        # Check if the file was uploaded within the expected time interval
        if now - recent_file_time > timedelta(minutes=max_time_interval):
            # Send notification if no recent file
            send_alert(sns_topic_arn, "No recent file has been uploaded to the bucket.")
        else:
            # Send notification with metadata if a recent file exists
            file_metadata = {
                "Prefix": expected_file_prefix,
                "Filename": recent_file['Key'],
                "Uploader": uploader,
                "Datetime_file_landed": recent_file_time.strftime('%Y-%m-%d %H:%M:%S'),
                "Datetime_lambda_ran": now.strftime('%Y-%m-%d %H:%M:%S')
            }
            send_metadata_notification(sns_topic_arn, file_metadata)

    except ClientError as e:
        print(e)
        send_alert(sns_topic_arn, f"An error occurred: {str(e)}")

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
