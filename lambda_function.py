#!/usr/bin/env python3
"""
Lambda function to update OpenSearch records with processed data from Bedrock batch inference.

This function reads the processed .jsonl.out files from S3, extracts the document ID and processed information
for each record, and updates the corresponding records in OpenSearch via a Lambda function.
"""

import os
import json
import logging
import io
import datetime
import time
import random
import re
import boto3
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
# Set to DEBUG level to see more detailed logs for troubleshooting
logger.setLevel(logging.DEBUG)

# OpenSearch parameters
OPENSEARCH_INDEX = os.environ.get('OPENSEARCH_INDEX', 'worko-jobs')
REGION = os.environ.get('AWS_REGION', 'ap-south-1')

# Lambda function names for different environments
OPENSEARCH_LAMBDA_DEV = os.environ.get('OPENSEARCH_LAMBDA_DEV', 'worko-job-app-dev-app-opensearch-operations')
OPENSEARCH_LAMBDA_PROD = os.environ.get('OPENSEARCH_LAMBDA_PROD', 'worko-job-app-prod-app-opensearch-operations')

# Vector index generation lambda function
VECTOR_INDEX_LAMBDA = os.environ.get('VECTOR_INDEX_LAMBDA')
VECTOR_INDEX_NAME = os.environ.get('VECTOR_INDEX_NAME')

# Environment setting
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'dev')

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
        except ClientError as e:
            # Check if we've exceeded max retries or if it's not a throttling error
            error_code = e.response.get('Error', {}).get('Code')
            if retries >= max_retries or error_code not in ['TooManyRequestsException', 'ThrottlingException']:
                raise
            
            # Calculate delay with exponential backoff and jitter
            delay = (2 ** retries) * base_delay + random.uniform(0, 0.5)
            logger.warning(f"Request throttled. Retrying in {delay:.2f} seconds... (Attempt {retries+1}/{max_retries})")
            time.sleep(delay)
            retries += 1

def get_opensearch_lambda_client():
    """
    Initialize and return a Lambda client to invoke OpenSearch operations.
    """
    # Determine which Lambda function to use based on environment
    lambda_client = boto3.client('lambda', region_name=REGION)
    
    return lambda_client

def parse_opensearch_response(response_payload):
    """
    Parse and extract relevant information from OpenSearch Lambda response.
    Handles different response formats.
    
    Args:
        response_payload: The payload from Lambda response
        
    Returns:
        tuple: (success, document_data, status_code, error_message)
    """
    success = False
    document_data = {}
    status_code = None
    error_message = None
    
    try:
        # Handle string or dict payload
        if isinstance(response_payload, str):
            try:
                response_payload = json.loads(response_payload)
            except json.JSONDecodeError:
                error_message = "Could not parse response payload as JSON"
                return success, document_data, status_code, error_message
        
        # Extract status code
        status_code = response_payload.get('statusCode')
        
        # Check for success based on status code
        if status_code == 200:
            success = True
        elif status_code == 404:
            error_message = "Document not found"
            return success, document_data, status_code, error_message
        
        # Extract document data from body
        body = response_payload.get('body')
        if body:
            if isinstance(body, str):
                try:
                    body = json.loads(body)
                except json.JSONDecodeError:
                    # If body is not JSON, it might be the document content directly
                    document_data = {'content': body}
                    return success, document_data, status_code, error_message
            
            # Handle OpenSearch specific response formats
            if isinstance(body, dict):
                # Check for OpenSearch result field
                if 'result' in body:
                    if body['result'] in ['created', 'updated']:
                        success = True
                
                # Extract document source if available
                if '_source' in body:
                    document_data = body['_source']
                else:
                    # If no _source, use the body as the document data
                    document_data = body
        
        # Check for result field directly in response
        if 'result' in response_payload:
            if response_payload['result'] in ['created', 'updated']:
                success = True
        
        # Check for error message
        if not success and not error_message:
            error_message = response_payload.get('error') or response_payload.get('message') or "Unknown error"
    
    except Exception as e:
        error_message = f"Error parsing response: {str(e)}"
    
    return success, document_data, status_code, error_message

def trigger_vector_index_generation(lambda_client, record_id):
    """
    Trigger vector index generation by calling another lambda function asynchronously.
    
    Args:
        lambda_client: The Lambda client to use
        record_id: The document ID that was updated
    """
    if not VECTOR_INDEX_LAMBDA or not VECTOR_INDEX_NAME:
        logger.warning("Vector index lambda function or index name not configured, skipping vector generation")
        return
    
    try:
        # First get the updated document from OpenSearch
        lambda_function_name = OPENSEARCH_LAMBDA_PROD if ENVIRONMENT.lower() == 'prod' else OPENSEARCH_LAMBDA_DEV
        
        get_payload = {
            "indexName": OPENSEARCH_INDEX,
            "body": {
                "documentId": record_id
            },
            "operation": "getDocumentById"
        }
        
        # Get the updated document
        get_response = retry_with_backoff(
            lambda_client.invoke,
            FunctionName=lambda_function_name,
            InvocationType='RequestResponse',
            Payload=json.dumps(get_payload)
        )
        
        get_result = json.loads(get_response['Payload'].read().decode())
        
        # Extract the full document structure (one level up from _source)
        body = get_result.get('body')
        if isinstance(body, str):
            body = json.loads(body)
        
        if not body:
            logger.error(f"Failed to retrieve document {record_id} for vector generation")
            return
        
        # Prepare payload for vector index generation - use the full document structure
        vector_payload = {
            "jobData": body,  # This contains the full OpenSearch document with _index, _id, _source etc.
            "indexName": VECTOR_INDEX_NAME
        }
        
        logger.info(f"Triggering vector index generation for document {record_id}")
        
        # Call vector index lambda function asynchronously
        lambda_client.invoke(
            FunctionName=VECTOR_INDEX_LAMBDA,
            InvocationType='Event',  # Async invocation
            Payload=json.dumps(vector_payload)
        )
        
        logger.info(f"Successfully triggered vector index generation for document {record_id}")
        logger.debug(f"Vector payload structure: {json.dumps(vector_payload, indent=2)[:500]}...")
        
    except Exception as e:
        logger.error(f"Error triggering vector index generation for document {record_id}: {e}")

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
    total_lines = 0
    skipped_lines = 0
    invalid_json_lines = 0
    missing_record_id_lines = 0
    missing_extracted_info_lines = 0
    
    try:
        logger.info(f"Reading inference output from s3://{bucket}/{key}")
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        
        lines = content.splitlines()
        total_lines = len(lines)
        logger.info(f"Total lines in file {key}: {total_lines}")
        
        # Process each line in the file
        for line_num, line in enumerate(lines, 1):
            if not line.strip():  # Skip empty lines
                skipped_lines += 1
                logger.debug(f"Skipping empty line {line_num}")
                continue
                
            try:
                data = json.loads(line)
                record_id = data.get('recordId')
                
                if not record_id:
                    missing_record_id_lines += 1
                    logger.warning(f"Line {line_num}: Missing recordId in data: {line[:200]}...")
                    continue
                
                # Extract the model output which contains the processed information
                model_output = data.get('modelOutput', {})
                content_items = model_output.get('content', [])
                
                # Extract the JSON from the text content
                extracted_info = None
                for item in content_items:
                    if item.get('type') == 'text':
                        text_content = item.get('text', '{}')
                        try:
                            extracted_info = json.loads(text_content)
                            break
                        except json.JSONDecodeError:
                            # Try to clean the text by removing comment patterns
                            try:
                                comment_matches = re.findall(r'//[^\n]*\n?', text_content)
                                if comment_matches:
                                    logger.info(f"Line {line_num}: Found comment patterns for record {record_id}: {comment_matches}")
                                cleaned_text = re.sub(r'//[^\n]*\n?', '', text_content)
                                extracted_info = json.loads(cleaned_text)
                                logger.info(f"Line {line_num}: Successfully parsed JSON after cleaning for record {record_id}")
                                break
                            except json.JSONDecodeError:
                                logger.warning(f"Line {line_num}: Could not parse JSON from content for record {record_id} even after cleaning. Content: {text_content[:200]}...")
                                continue
                
                if not extracted_info:
                    missing_extracted_info_lines += 1
                    logger.warning(f"Line {line_num}: Missing extracted_info for record {record_id}. Model output: {json.dumps(model_output)[:300]}...")
                    continue
                
                records.append({
                    'record_id': record_id,
                    'extracted_info': extracted_info
                })
                    
            except json.JSONDecodeError:
                invalid_json_lines += 1
                logger.error(f"Line {line_num}: Invalid JSON: {line[:200]}...")
            except Exception as e:
                skipped_lines += 1
                logger.error(f"Line {line_num}: Error processing line: {e}. Line content: {line[:200]}...")
    
    except Exception as e:
        logger.error(f"Error reading inference output file from S3: {e}")
    
    # Log detailed statistics
    processed_records = len(records)
    logger.info(f"File {key} processing summary:")
    logger.info(f"  Total lines: {total_lines}")
    logger.info(f"  Successfully processed records: {processed_records}")
    logger.info(f"  Skipped empty/error lines: {skipped_lines}")
    logger.info(f"  Invalid JSON lines: {invalid_json_lines}")
    logger.info(f"  Missing recordId lines: {missing_record_id_lines}")
    logger.info(f"  Missing extracted_info lines: {missing_extracted_info_lines}")
    logger.info(f"  Total unprocessed lines: {total_lines - processed_records}")
    
    if total_lines != processed_records:
        logger.warning(f"MISMATCH: Expected {total_lines} records but only processed {processed_records} records from file {key}")
    
    return records

def update_opensearch_records(lambda_client, records):
    """
    Update OpenSearch records with the extracted information via Lambda function.
    """
    updated_count = 0
    not_found_records = []
    skipped_already_updated = 0
    processing_errors = 0
    
    logger.info(f"Starting to update {len(records)} OpenSearch records")
    # Log the first record structure for debugging
    if records and len(records) > 0:
        logger.debug(f"Sample record structure: {json.dumps(records[0], indent=2)[:500]}...")
    
    # Determine which Lambda function to use based on environment
    lambda_function_name = OPENSEARCH_LAMBDA_PROD if ENVIRONMENT.lower() == 'prod' else OPENSEARCH_LAMBDA_DEV
    
    for record_index, record in enumerate(records, 1):
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
        
        # Add isJobUpdated field with value true
        cleaned_info['isJobUpdated'] = True
        
        try:
            # First check if the document exists with retry logic
            try:
                # Prepare payload for getDocumentById operation
                get_payload = {
                    "indexName": OPENSEARCH_INDEX,
                    "body": {
                        "documentId": record_id
                    },
                    "operation": "getDocumentById"
                }
                
                logger.debug(f"Checking if document {record_id} exists with payload: {get_payload}")
                
                # Invoke Lambda function to check if document exists
                get_response = retry_with_backoff(
                    lambda_client.invoke,
                    FunctionName=lambda_function_name,
                    InvocationType='RequestResponse',
                    Payload=json.dumps(get_payload)
                )
                
                # Parse the response
                get_result = json.loads(get_response['Payload'].read().decode())
                
                # Log the response for debugging
                logger.debug(f"Get document response for {record_id}: {get_result}")
                
                # Use helper function to parse the response
                exists, doc_source, status_code, error = parse_opensearch_response(get_result)
                
                # Check if document exists
                if status_code == 404 or not exists:
                    logger.warning(f"Document with ID {record_id} not found in index {OPENSEARCH_INDEX}: {error}")
                    not_found_records.append(record_id)
                    continue
                
                logger.debug(f"Extracted document source for {record_id}: {doc_source}")
                
                # Check if document already has isJobUpdated field set to true
                if doc_source.get('isJobUpdated') == True:
                    skipped_already_updated += 1
                    logger.info(f"Record {record_index}/{len(records)}: Skipping document {record_id} as it's already updated (isJobUpdated=true)")
                    continue
                    
            except Exception as e:
                logger.error(f"Error checking document {record_id}: {e}")
                not_found_records.append(record_id)
                continue
            
            # Prepare payload for updateDocumentById operation
            update_payload = {
                "indexName": OPENSEARCH_INDEX,
                "body": {
                    "id": record_id,
                    "updatedDto": cleaned_info
                },
                "operation": "updateDocumentById"
            }
            
            logger.debug(f"Updating document {record_id} with payload: {update_payload}")
            
            # Invoke Lambda function to update the document
            update_response = retry_with_backoff(
                lambda_client.invoke,
                FunctionName=lambda_function_name,
                InvocationType='RequestResponse',
                Payload=json.dumps(update_payload)
            )
            
            # Parse the response
            update_result = json.loads(update_response['Payload'].read().decode())
            
            # Log the response for debugging
            logger.debug(f"Update document response for {record_id}: {update_result}")
            
            # Log the raw response for debugging
            logger.debug(f"Raw update response for {record_id}: {json.dumps(update_result)}")
            
            # Check for successful update based on common response patterns
            success = False
            
            # Handle different response formats
            if update_result.get('statusCode') == 200:
                success = True
            elif update_result.get('result') == 'updated':
                success = True
            elif isinstance(update_result.get('body'), dict) and update_result.get('body').get('result') == 'updated':
                success = True
            elif isinstance(update_result.get('body'), str):
                try:
                    body_json = json.loads(update_result['body'])
                    if body_json.get('result') == 'updated':
                        success = True
                except json.JSONDecodeError:
                    pass
            
            if success:
                updated_count += 1
                logger.info(f"Record {record_index}/{len(records)}: Successfully updated document {record_id}")
                
                # Trigger vector index generation after successful update
                trigger_vector_index_generation(lambda_client, record_id)
            else:
                processing_errors += 1
                error_msg = "Unknown error"
                if update_result.get('body') and isinstance(update_result.get('body'), str):
                    try:
                        body_json = json.loads(update_result['body'])
                        error_msg = body_json.get('error', error_msg)
                    except json.JSONDecodeError:
                        pass
                
                logger.warning(f"Record {record_index}/{len(records)}: Update for document {record_id} failed: {error_msg} (Status: {update_result.get('statusCode')})")
                logger.debug(f"Full error response: {update_result}")
                
        except Exception as e:
            processing_errors += 1
            logger.error(f"Record {record_index}/{len(records)}: Error updating document {record_id}: {e}")
    
    # Log detailed processing summary
    total_processed = updated_count + len(not_found_records) + skipped_already_updated + processing_errors
    logger.info(f"OpenSearch update summary:")
    logger.info(f"  Total records to process: {len(records)}")
    logger.info(f"  Successfully updated: {updated_count}")
    logger.info(f"  Not found in OpenSearch: {len(not_found_records)}")
    logger.info(f"  Already updated (skipped): {skipped_already_updated}")
    logger.info(f"  Processing errors: {processing_errors}")
    logger.info(f"  Total processed: {total_processed}")
    
    if total_processed != len(records):
        logger.error(f"PROCESSING MISMATCH: Expected to process {len(records)} records but only processed {total_processed}")
    
    # Log records that were not found
    if not_found_records:
        try:
            with open(NOT_FOUND_LOG_FILE, 'w') as f:
                for record_id in not_found_records:
                    f.write(f"{record_id}\n")
            logger.info(f"Logged {len(not_found_records)} not found records to {NOT_FOUND_LOG_FILE}")
            
            # Optionally upload the log file to S3
            s3_client = boto3.client('s3')
            log_key = f"logs/not_found_records_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
            s3_client.upload_file(NOT_FOUND_LOG_FILE, S3_BUCKET, log_key)
            logger.info(f"Uploaded not found records log to s3://{S3_BUCKET}/{log_key}")
        except Exception as e:
            logger.error(f"Error writing to not found log file: {e}")
    
    return updated_count, not_found_records

def lambda_handler(event, context):
    """
    AWS Lambda function handler.
    """
    if not S3_BUCKET:
        error_msg = "Missing required environment variable: S3_BUCKET must be set."
        logger.error(error_msg)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': error_msg})
        }
    
    try:
        logger.info(f"Initializing Lambda client for OpenSearch operations using {ENVIRONMENT} environment...")
        lambda_client = get_opensearch_lambda_client()
        
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
                logger.info(f"Processing {len(records)} records from file {s3_key} for update...")
                updated_count, not_found_records = update_opensearch_records(lambda_client, records)
                total_updated += updated_count
                all_not_found.extend(not_found_records)
                logger.info(f"File {s3_key} update summary: {updated_count} updated, {len(not_found_records)} not found")
            else:
                logger.warning(f"No valid records found in file {s3_key} - check file format and content")
        
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
