import boto3
import gzip
import io
import re
from datetime import datetime, timezone, timedelta

def fetch_s3_logs(bucket_name, prefix=''):
    s3 = boto3.client('s3')
    logs = []
    
    # List objects in the specified S3 bucket
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    current_time = datetime.now(timezone.utc)
    one_hour_ago = current_time - timedelta(hours=1)

    for obj in response.get('Contents', []):
        log_key = obj['Key']
        last_modified = obj['LastModified']  # Extract LastModified time
        
        # Check if the log file was modified within the last hour
        if last_modified >= one_hour_ago:
            log_data = s3.get_object(Bucket=bucket_name, Key=log_key)['Body'].read()
            logs.extend((log_data.splitlines(), last_modified))  # Store logs with LastModified

    return logs

def extract_uploader_info(logs):
    uploader_info = []
    put_pattern = re.compile(r'PUT')

    current_time = datetime.now(timezone.utc)

    for log_lines, last_modified in logs:
        for log in log_lines:
            log_str = log.decode('utf-8')
            if put_pattern.search(log_str):
                username_pattern = re.compile(r'\s([A-Za-z0-9._-]+)\s+\[.*?\]')  # Adjust as needed
                match = username_pattern.search(log_str)
                if match:
                    username = match.group(1)
                    time_difference = current_time - last_modified
                    uploader_info.append((log_str, username, time_difference))

    return uploader_info

def main():
    bucket_name = 'aws-logs-useast01'  # Replace with your S3 bucket name
    prefix = 'Awslogs/s3/'              # Optional: Replace with the prefix for logs if necessary

    # Fetch logs from the S3 bucket
    logs = fetch_s3_logs(bucket_name, prefix)
    
    print(logs)
    # Extract uploader information from PUT events
    # uploader_info = extract_uploader_info(logs)

    # Print the uploader information and time differences
    # for log_entry, username, time_difference in uploader_info:
    #     print(f'Log Entry: {log_entry}, Uploader Username: {username}, Time Difference: {time_difference}')

if __name__ == '__main__':
    main()
