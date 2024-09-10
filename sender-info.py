import boto3
from datetime import datetime, timedelta
import json

def get_uploader_of_s3_file(bucket_name, filename, start_time, end_time):
    # Initialize a CloudTrail client
    client = boto3.client('cloudtrail')
    
    # Lookup CloudTrail events
    response = client.lookup_events(
        LookupAttributes=[
            {
                'AttributeKey': 'EventSource',
                'AttributeValue': 's3.amazonaws.com'
            },
            {
                'AttributeKey': 'EventName',
                'AttributeValue': 'PutObject'
            },
        ],
        StartTime=start_time,
        EndTime=end_time,
        MaxResults=50  # Adjust this based on your needs
    )
    
    for event in response.get('Events', []):
        event_data = json.loads(event['CloudTrailEvent'])
        request_params = event_data.get('requestParameters', {})
        bucket = request_params.get('bucketName')
        key = request_params.get('key')
        
        if bucket == bucket_name and key == filename:
            user_identity = event_data.get('userIdentity', {})
            username = user_identity.get('userName', 'Unknown')
            principal_id = user_identity.get('arn', 'Unknown')
            print(f"File: {filename} was uploaded by User: {username}, ARN: {principal_id}")
            return username, principal_id
    
    print(f"No upload event found for file: {filename}")
    return None, None

# Example usage
bucket_name = 'athena-glue-1205'
filename = 'new.py'
start_time = datetime.now() - timedelta(days=1)  # Adjust time range as needed
end_time = datetime.now()

get_uploader_of_s3_file(bucket_name, filename, start_time, end_time)
