import boto3

s3_client = boto3.client('s3')


def list_all_objects(bucketname, prefix):
    objects = []
    continuation_token = None

    while True:
        if continuation_token:
            response = s3_client.list_objects_v2(
                Bucket=bucketname,
                Prefix=prefix,
                ContinuationToken=continuation_token
            )
        else:
            response = s3_client.list_objects_v2(
                Bucket=bucketname,
                Prefix=prefix
            )

        if 'Contents' in response:
            objects.extend(response['Contents'])
        
        if response.get('IsTruncated'):  # Check if there are more objects
            continuation_token = response['NextContinuationToken']
        else:
            break

    return objects


bucketname = "athena-glue-1205"
prefix = "csv/logs/"
logs = list_all_objects(bucketname,prefix)

print(logs)

