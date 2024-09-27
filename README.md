This Lambda function monitors multiple NFL S3 buckets (with thier prefix) mentioned in NFL excel sheet for recent file uploads, retrieves file metadata as per ticket requirement, and writes this data into a CSV file stored in a specified S3 bucket. It also sends notifications via SNS if files are uploaded or if there are any issues. 
<br>

Functions used:

- lambda_handler(): This function only calls `main()` and `write_csv_to_s3()`

- write_csv_to_s3(): This function take three parameters such as (`csv_data` here we write all file_metadata, `output_bucket` S3 bucket where we want to store csv, `output_csv_key` name of the csv file to store in S3)

- send_notification(): This function take two parameters such as (`sns_topic_arn` topic arn is provided as lambda environment variable, `metadata` here we write file_metadata to be sent via email)

- fetch_logs(): This function take four parameters such as (`log_bucket` bucket name where CloudTrail stores the logs, `log_prefix` to filter logs to scan only todays logs, `file_key` filename of NFL S3 object, `bucket_name` NFL bucket name)

In order to execute, it will list all today's uploaded/modified objects/logs from CloudTrail S3 logs by using `response = s3_client.list_objects_v2(Bucket=log_bucket, Prefix=log_prefix)` then iterate each log file by 
```
  # Retrieve the contents (logs) from the response
  logs = response.get('Contents', [])
  
  # Iterate through each log and filter based on the LastModified time
  for log in logs:
      last_modified_time_logs = log['LastModified']

      # Check if the log was modified within the 'max_time_interval' (e.g., 3 hours)
      if now - last_modified_time_logs <= timedelta(hours=max_time_interval):
          log_key = log['Key']
          log_obj = s3_client.get_object(Bucket=log_bucket, Key=log_key)
          log_content = log_obj['Body'].read()
```

After this, it decompress gzipped logs into `utf-8`. Then it iterate each line from logs to find `PutObject` event, if it find event it use `if request_params.get('bucketName') == bucket_name and request_params.get('key') == file_key` this condition to validate that this logs is for nfl bucket and object. If yes, it extracs the uploader name from the logs and return this value `return user_identity.get('arn', 'Unknown').split('/')[-1]`.

```
for line in log_lines:
    event_data = json.loads(line)
    for record in event_data.get('Records', []):
        if record.get('eventName') == 'PutObject':
```

- main(): This is main function that interate all NFL buckets and seprate it's bucket name and prefix by the folowwing code
```
for nfl_bucket in bucket_names:
    bucket_name, prefix = nfl_bucket.split(':')
```
`bucket_names` variable is taking values from lambda environment variables is this format `bucket01:prefix01/,bucket02:prefix02/` each bucket is seprated by `,` and prefix is seprated by `:` the it lists objects from bucket with specified prefix
```
# List objects in the bucket
response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
```
After some sanity checks it extracts filename and LastModified timestamp of each file
```
for obj in response['Contents']:
    file_key = obj['Key']
    last_modified_time = obj['LastModified']
```
After extracting filename and LastModified timestamp of each file, if the file was uploaded within the expected time interval, it will call `fetch_logs()` to extract uploader name, then it will save `file_metadata` and send notification also append in csv_data. `hours=max_time_interval` this value is coming from lambda environment variable.
```
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

    send_notification(sns_topic_arn, file_metadata)
    csv_data.append([bucket_name, file_metadata["Prefix"], file_metadata["Filename"], file_metadata["Uploader"], file_metadata["Datetime_file_landed"], file_metadata["Datetime_lambda_ran"]])
    
```
