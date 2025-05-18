#!/usr/bin/env python3
"""
Script to update OpenSearch records with processed data from Bedrock batch inference.

This script reads the processed file-1.jsonl.out from Bedrock batch inference,
extracts the document ID and processed information for each record,
and updates the corresponding records in OpenSearch.
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
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# OpenSearch connection parameters
# These should match the environment variables used in the worko-job-app
OPENSEARCH_ENDPOINT = os.environ.get('APP_OPENSEARCH_ENDPOINT', 'https://43.205.212.11/')
OPENSEARCH_INDEX = 'worko-jobs'
REGION = os.environ.get('APP_SLS_REGION', 'ap-south-1')

# Local development settings
IS_LOCAL = True  # Force local mode to use basic auth
OPENSEARCH_USERNAME = os.environ.get('APP_OPENSEARCH_DASHBOARD_USER_NAME', 'master')
OPENSEARCH_PASSWORD = os.environ.get('APP_OPENSEARCH_DASHBOARD_PASSWORD', 'ThisisMaster@2025')

# S3 configuration
S3_BUCKET = os.environ.get('S3_BUCKET', 'worko-batch-inference-dev')
S3_PREFIX = os.environ.get('S3_PREFIX', 'output/')  # Directory path in S3 bucket

# File paths
NOT_FOUND_LOG_FILE = 'not_found_records.log'

# For testing purposes, set this to False to actually connect to OpenSearch
SIMULATE_UPDATES = False

def retry_with_backoff(func, *args, max_retries=5, base_delay=1, **kwargs):
    """
    Execute a function with exponential backoff retry logic.
    
    Args:
        func: Function to execute
        *args: Arguments to pass to the function
        max_retries: Maximum number of retries
        base_delay: Base delay in seconds
        **kwargs: Keyword arguments to pass to the function
        
    Returns:
        Result of the function call
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
    Initialize and return an OpenSearch client based on environment.
    """
    if IS_LOCAL and OPENSEARCH_USERNAME and OPENSEARCH_PASSWORD:
        # Local development with username/password
        client = OpenSearch(
            hosts=[OPENSEARCH_ENDPOINT],
            http_auth=(OPENSEARCH_USERNAME, OPENSEARCH_PASSWORD),
            use_ssl=True,
            verify_certs=False,
            connection_class=RequestsHttpConnection
        )
    else:
        # AWS environment with IAM authentication
        # Use the default credential chain
        credentials = boto3.Session().get_credentials()
        awsauth = AWS4Auth(
            credentials.access_key,
            credentials.secret_key,
            REGION,
            'es',
            session_token=credentials.token
        )
        
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
    
    Args:
        bucket: S3 bucket name
        prefix: S3 prefix (directory path)
        
    Returns:
        List of S3 object keys
    """
    # Use the default credential chain
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
    
    Args:
        bucket: S3 bucket name
        key: S3 object key
        
    Returns:
        List of dictionaries containing document ID and extracted information
    """
    records = []
    # Use the default credential chain
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
    
    Args:
        client: OpenSearch client
        records: List of records with document IDs and extracted information
        
    Returns:
        Tuple of (updated_count, not_found_records)
    """
    updated_count = 0
    not_found_records = []
    
    # If simulation mode is enabled, write updates to a file instead of connecting to OpenSearch
    if SIMULATE_UPDATES:
        updates = []
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
            
            # Create a simulated update operation
            update = {
                'index': OPENSEARCH_INDEX,
                'id': record_id,
                'update': cleaned_info  # Store directly in document, not in extractedInfo field
            }
            updates.append(update)
            logger.info(f"Simulated update for document {record_id}")
        
        # Write the updates to a file
        try:
            output_filename = f"opensearch_updates_{os.path.basename(records[0]['record_id'] if records else 'empty')}.json"
            with open(output_filename, 'w') as f:
                json.dump(updates, f, indent=2)
            logger.info(f"Wrote {len(updates)} simulated updates to {output_filename}")
            return len(updates), []
        except Exception as e:
            logger.error(f"Error writing to test output file: {e}")
            return 0, []
    
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
    return updated_count, not_found_records

def main():
    """Main function to execute the script."""
    if not OPENSEARCH_ENDPOINT:
        logger.error("OpenSearch endpoint not configured. Please set APP_OPENSEARCH_ENDPOINT environment variable.")
        return
    
    logger.info("Connecting to OpenSearch...")
    client = get_opensearch_client()
    
    try:
        # List all files in the S3 bucket with the specified prefix
        s3_files = list_s3_files(S3_BUCKET, S3_PREFIX)
        
        if not s3_files:
            logger.warning(f"No files found in S3 bucket {S3_BUCKET} with prefix {S3_PREFIX}")
            logger.warning("No local file processing will be attempted. Exiting.")
            return
        
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
        
        # Log records that were not found
        if all_not_found:
            try:
                with open(NOT_FOUND_LOG_FILE, 'w') as f:
                    for record_id in all_not_found:
                        f.write(f"{record_id}\n")
                logger.info(f"Logged {len(all_not_found)} not found records to {NOT_FOUND_LOG_FILE}")
            except Exception as e:
                logger.error(f"Error writing to not found log file: {e}")
    
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
    
    logger.info("Process completed successfully.")

if __name__ == "__main__":
    main()
