#!/bin/bash

# Define the string you're looking for
search_string="Hershey"

# List all buckets
buckets=$(aws s3api list-buckets --query "Buckets[].Name" --output text)

# Loop through all the buckets
for bucket in $buckets; do
    echo "Searching in bucket: $bucket"
    
    # Initialize pagination
    continuation_token=""
    
    # Loop through pages of objects in the bucket
    while : ; do
        if [ -z "$continuation_token" ]; then
            # First page
            response=$(aws s3api list-objects-v2 --bucket $bucket --query "Contents[].Key" --output text)
        else
            # Subsequent pages
            response=$(aws s3api list-objects-v2 --bucket $bucket --query "Contents[].Key" --output text --starting-token "$continuation_token")
        fi
        
        # Search for the string using grep
        echo "$response" | grep -i "$search_string" | while read key; do
            # Output the full S3 URI
            echo "s3://$bucket/$key"
        done
        
        # Check if there is a continuation token to fetch the next page
        continuation_token=$(echo "$response" | grep -o 'NextToken.*' | awk -F'"' '{print $4}')
        
        # If no continuation token is found, exit the loop
        if [ -z "$continuation_token" ]; then
            break
        fi
    done
done
