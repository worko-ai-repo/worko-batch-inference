#!/usr/bin/env python3
"""
Lambda function to update OpenSearch records with processed data from Bedrock batch inference.

This function reads the processed .jsonl.out files from S3, extracts the document ID and processed information
for each record, and updates the corresponding records in OpenSearch.
"""

import os
import json
import logging
import io
import datetime
import time
import random
from opensearchpy import OpenSearch, RequestsHttpConnection
from opensearchpy.exceptions import NotFoundError, ConnectionTimeout, TransportError
from requests_aws4auth import AWS4Auth
import boto3

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# OpenSearch connection parameters
OPENSEARCH_ENDPOINT = os.environ.get('OPENSEARCH_ENDPOINT')
OPENSEARCH_INDEX = os.environ.get('OPENSEARCH_INDEX', 'worko-jobs')
REGION = os.environ.get('AWS_REGION', 'ap-south-1')

# S3 configuration
S3_BUCKET = os.environ.get('S3_BUCKET')
S3_PREFIX = os.environ.get('S3_PREFIX', 'output/')

# File paths
NOT_FOUND_LOG_FILE = '/tmp/not_found_records.log'

def retry_with_backoff(func, *args, max_retries=5, base_delay=1, **kwargs):
    """
    Execute a function with exponential backoff retry logic.
    """
    retries = 0
    while True:
        try:
            return func(*args, **kwargs)
        except (ConnectionTimeout, TransportError) as e:
            # Check if we've exceeded max retries or if it's not a throttling error
            if retries >= max_retries or not (hasattr(e, 'status_code') and e.status_code == 429):
                raise
            
            # Calculate delay with exponential backoff and jitter
            delay = (2 ** retries) * base_delay + random.uniform(0, 0.5)
            logger.warning(f"Request throttled. Retrying in {delay:.2f} seconds... (Attempt {retries+1}/{max_retries})")
            time.sleep(delay)
            retries += 1

def get_opensearch_client():
    """
    Initialize and return an OpenSearch client.
    """
    # AWS environment with IAM authentication
    credentials = boto3.Session().get_credentials()
    
    # Create AWS Signature V4 auth
    awsauth = AWS4Auth(
        credentials.access_key,
        credentials.secret_key,
        REGION,
        'es',
        session_token=credentials.token
    )
    
    # Use AWS Signature Version 4 Signer for all requests to OpenSearch
    client = OpenSearch(
        hosts=[OPENSEARCH_ENDPOINT],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=False,  # Set to False to ignore self-signed certificate errors
        connection_class=RequestsHttpConnection
    )
    
    return client

def list_s3_files(bucket, prefix):
    """
    List all files in an S3 bucket with the given prefix.
    """
    s3_client = boto3.client('s3', region_name=REGION)
    
    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix
        )
        
        if 'Contents' not in response:
            logger.warning(f"No files found in s3://{bucket}/{prefix}")
            return []
        
        # Get current date in UTC
        today = datetime.datetime.now(datetime.timezone.utc).date()
        
        # Extract the keys (file paths) from the response
        # Filter by file extension (.jsonl.out) and creation date (today)
        files = []
        for obj in response.get('Contents', []):
            key = obj['Key']
            last_modified = obj['LastModified'].date()
            
            if key.endswith('.jsonl.out') and last_modified == today:
                files.append(key)
        
        logger.info(f"Found {len(files)} files created today with .jsonl.out extension in s3://{bucket}/{prefix}")
        return files
    
    except Exception as e:
        logger.error(f"Error listing files in S3 bucket {bucket}/{prefix}: {e}")
        return []

def read_inference_output_from_s3(bucket, key):
    """
    Read the Bedrock batch inference output file from S3 and extract document IDs and processed data.
    """
    records = []
    s3_client = boto3.client('s3', region_name=REGION)
    
    try:
        logger.info(f"Reading inference output from s3://{bucket}/{key}")
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        
        # Process each line in the file
        for line in content.splitlines():
            try:
                data = json.loads(line)
                record_id = data.get('recordId')
                
                # Extract the model output which contains the processed information
                model_output = data.get('modelOutput', {})
                content_items = model_output.get('content', [])
                
                # Extract the JSON from the text content
                extracted_info = None
                for item in content_items:
                    if item.get('type') == 'text':
                        try:
                            extracted_info = json.loads(item.get('text', '{}'))
                            break
                        except json.JSONDecodeError:
                            logger.warning(f"Could not parse JSON from content for record {record_id}")
                
                if record_id and extracted_info:
                    records.append({
                        'record_id': record_id,
                        'extracted_info': extracted_info
                    })
                else:
                    logger.warning(f"Missing record_id or extracted_info in line: {line[:100]}...")
                    
            except json.JSONDecodeError:
                logger.error(f"Invalid JSON in line: {line[:100]}...")
            except Exception as e:
                logger.error(f"Error processing line: {e}")
    
    except Exception as e:
        logger.error(f"Error reading inference output file from S3: {e}")
    
    logger.info(f"Processed {len(records)} records from inference output file {key}")
    return records

def update_opensearch_records(client, records):
    """
    Update OpenSearch records with the extracted information.
    """
    updated_count = 0
    not_found_records = []
    
    for record in records:
        record_id = record['record_id']
        extracted_info = record['extracted_info']
        
        # Remove null values from extracted_info
        cleaned_info = {}
        for key, value in extracted_info.items():
            if value is not None:
                if isinstance(value, dict):
                    # For nested dictionaries like salaryRange, experienceRange
                    nested_cleaned = {k: v for k, v in value.items() if v is not None}
                    if nested_cleaned:  # Only add if there are non-null values
                        cleaned_info[key] = nested_cleaned
                else:
                    cleaned_info[key] = value
        
        try:
            # First check if the document exists with retry logic
            try:
                document_exists = retry_with_backoff(
                    client.get,
                    index=OPENSEARCH_INDEX,
                    id=record_id
                )
            except NotFoundError:
                logger.warning(f"Document with ID {record_id} not found in index {OPENSEARCH_INDEX}")
                not_found_records.append(record_id)
                continue
            
            # Update the document with extracted information directly with retry logic
            update_body = {
                "doc": cleaned_info  # Store directly in document, not in extractedInfo field
            }
            
            response = retry_with_backoff(
                client.update,
                index=OPENSEARCH_INDEX,
                id=record_id,
                body=update_body,
                refresh=True
            )
            
            if response.get('result') == 'updated':
                updated_count += 1
                logger.debug(f"Successfully updated document {record_id}")
            else:
                logger.warning(f"Update for document {record_id} returned unexpected result: {response.get('result')}")
                
        except Exception as e:
            logger.error(f"Error updating document {record_id}: {e}")
    
    logger.info(f"Updated {updated_count} documents in OpenSearch")
    
    # Log records that were not found
    if not_found_records:
        try:
            with open(NOT_FOUND_LOG_FILE, 'w') as f:
                for record_id in not_found_records:
                    f.write(f"{record_id}\n")
            logger.info(f"Logged {len(not_found_records)} not found records to {NOT_FOUND_LOG_FILE}")
            
            # Optionally upload the log file to S3
            s3_client = boto3.client('s3')
            log_key = f"{S3_PREFIX}logs/not_found_records_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
            s3_client.upload_file(NOT_FOUND_LOG_FILE, S3_BUCKET, log_key)
            logger.info(f"Uploaded not found records log to s3://{S3_BUCKET}/{log_key}")
        except Exception as e:
            logger.error(f"Error writing to not found log file: {e}")
    
    return updated_count, not_found_records

def lambda_handler(event, context):
    """
    AWS Lambda function handler.
    """
    if not OPENSEARCH_ENDPOINT or not S3_BUCKET:
        error_msg = "Missing required environment variables: OPENSEARCH_ENDPOINT and S3_BUCKET must be set."
        logger.error(error_msg)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': error_msg})
        }
    
    try:
        logger.info("Connecting to OpenSearch...")
        client = get_opensearch_client()
        
        # List all files in the S3 bucket with the specified prefix
        s3_files = list_s3_files(S3_BUCKET, S3_PREFIX)
        
        if not s3_files:
            logger.warning(f"No files found in S3 bucket {S3_BUCKET} with prefix {S3_PREFIX}")
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'No files to process'})
            }
        
        total_updated = 0
        all_not_found = []
        
        # Process each file in the S3 bucket
        for s3_key in s3_files:
            logger.info(f"Processing file: {s3_key}")
            
            # Read records from the S3 file
            records = read_inference_output_from_s3(S3_BUCKET, s3_key)
            
            if records:
                logger.info(f"Updating OpenSearch records for file {s3_key}...")
                updated_count, not_found_records = update_opensearch_records(client, records)
                total_updated += updated_count
                all_not_found.extend(not_found_records)
            else:
                logger.info(f"No records found in file {s3_key}.")
        
        logger.info(f"Total records updated across all files: {total_updated}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Process completed successfully',
                'totalUpdated': total_updated,
                'totalNotFound': len(all_not_found)
            })
        }
    
    except Exception as e:
        error_msg = f"Error in Lambda execution: {str(e)}"
        logger.error(error_msg)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': error_msg})
        }
