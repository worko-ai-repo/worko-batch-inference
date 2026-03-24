#!/usr/bin/env python3
"""
Lambda function to update GCP Firestore records with processed data from GCP Vertex AI batch inference.

This function reads the processed .jsonl.out files from S3, extracts the document ID and processed information
for each record, and updates the corresponding records in GCP Firestore.
"""

import os
import json
import logging
import datetime
import re
import boto3
from google.cloud import storage
from google.cloud import firestore
from google.oauth2 import service_account

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment setting
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'dev')
REGION = os.environ.get('AWS_REGION', 'ap-south-1')

# GCP Firestore parameters
GCP_PROJECT_ID = f"worko-{ENVIRONMENT}"
FIRESTORE_DB_NAME = f"worko-jobs-db-{ENVIRONMENT}"
FIRESTORE_COLLECTION = "workoJobs"
GCP_SECRET_NAME = f"GCP-firestore-secret-{ENVIRONMENT}"

# Vector index generation lambda function
VECTOR_INDEX_LAMBDA = os.environ.get('VECTOR_INDEX_LAMBDA')
VECTOR_INDEX_NAME = os.environ.get('VECTOR_INDEX_NAME')

# GCS configuration (not used anymore)
# S3_BUCKET = os.environ.get('S3_BUCKET')
# S3_PREFIX = os.environ.get('S3_PREFIX', 'output/')

# File paths
NOT_FOUND_LOG_FILE = '/tmp/not_found_records.log'

def get_gcp_credentials():
    """
    Retrieve GCP Firestore credentials from AWS Secrets Manager.
    """
    secrets_client = boto3.client('secretsmanager', region_name=REGION)
    
    try:
        response = secrets_client.get_secret_value(SecretId=GCP_SECRET_NAME)
        secret_string = response['SecretString']
        credentials_dict = json.loads(secret_string)
        
        credentials = service_account.Credentials.from_service_account_info(credentials_dict)
        logger.info(f"Successfully retrieved GCP credentials from secret: {GCP_SECRET_NAME}")
        return credentials
    except Exception as e:
        logger.error(f"Error retrieving GCP credentials: {e}")
        raise

def get_firestore_client():
    """
    Initialize and return a Firestore client.
    """
    credentials = get_gcp_credentials()
    client = firestore.Client(
        project=GCP_PROJECT_ID,
        database=FIRESTORE_DB_NAME,
        credentials=credentials
    )
    logger.info(f"Initialized Firestore client for project: {GCP_PROJECT_ID}, database: {FIRESTORE_DB_NAME}")
    return client

def trigger_vector_index_generation(record_id, doc_data):
    """
    Trigger vector index generation by calling another lambda function asynchronously.
    
    Args:
        record_id: The document ID that was updated
        doc_data: The updated document data from Firestore
    """
    if not VECTOR_INDEX_LAMBDA or not VECTOR_INDEX_NAME:
        logger.warning("Vector index lambda function or index name not configured, skipping vector generation")
        return
    
    try:
        lambda_client = boto3.client('lambda', region_name=REGION)
        
        # Prepare payload for vector index generation
        vector_payload = {
            "jobData": {
                "_id": record_id,
                "_source": doc_data
            },
            "indexName": VECTOR_INDEX_NAME
        }
        
        logger.info(f"Triggering vector index generation for document {record_id}")
        
        # Call vector index lambda function asynchronously
        lambda_client.invoke(
            FunctionName=VECTOR_INDEX_LAMBDA,
            InvocationType='Event',
            Payload=json.dumps(vector_payload)
        )
        
        logger.info(f"Successfully triggered vector index generation for document {record_id}")
        
    except Exception as e:
        logger.error(f"Error triggering vector index generation for document {record_id}: {e}")

def read_inference_output_from_gcs(bucket_name, file_name):
    """
    Read the GCP Vertex AI batch inference output file from GCS and extract document IDs and processed data.
    """
    records = []
    credentials = get_gcp_credentials()
    storage_client = storage.Client(credentials=credentials)
    total_lines = 0
    skipped_lines = 0
    invalid_json_lines = 0
    missing_record_id_lines = 0
    missing_extracted_info_lines = 0
    truncated_responses = 0
    
    try:
        logger.info(f"Reading inference output from gs://{bucket_name}/{file_name}")
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        content = blob.download_as_text()
        
        lines = content.splitlines()
        total_lines = len(lines)
        logger.info(f"Total lines in file {file_name}: {total_lines}")
        
        # Process each line in the file
        for line_num, line in enumerate(lines, 1):
            if not line.strip():
                skipped_lines += 1
                logger.debug(f"Skipping empty line {line_num}")
                continue
                
            try:
                data = json.loads(line)
                record_id = data.get('custom_id')
                
                if not record_id:
                    missing_record_id_lines += 1
                    logger.warning(f"Line {line_num}: Missing custom_id in data: {line[:200]}...")
                    continue
                
                # Extract the model output from GCP Vertex AI response structure
                response_data = data.get('response', {})
                candidates = response_data.get('candidates', [])
                
                # Check for truncated responses and skip them
                if candidates and candidates[0].get('finishReason') == 'MAX_TOKENS':
                    truncated_responses += 1
                    logger.warning(f"Line {line_num}: Response truncated (MAX_TOKENS) for record {record_id}, skipping")
                    continue
                
                # Extract the JSON from the content
                extracted_info = None
                for candidate in candidates:
                    content_data = candidate.get('content', {})
                    parts = content_data.get('parts', [])
                    
                    for part in parts:
                        text_content = part.get('text', '{}')
                        if text_content:
                            try:
                                extracted_info = json.loads(text_content)
                                break
                            except json.JSONDecodeError:
                                # Strip markdown code blocks (```json and ```)
                                cleaned_text = text_content.strip()
                                if cleaned_text.startswith('```'):
                                    # Remove opening ```json or ```
                                    cleaned_text = re.sub(r'^```(?:json)?\s*\n?', '', cleaned_text)
                                    # Remove closing ```
                                    cleaned_text = re.sub(r'\n?```\s*$', '', cleaned_text)
                                # Remove comment patterns
                                cleaned_text = re.sub(r'//[^\n]*', '', cleaned_text)
                                try:
                                    extracted_info = json.loads(cleaned_text.strip())
                                    break
                                except json.JSONDecodeError:
                                    logger.warning(f"Line {line_num}: Could not parse JSON from content for record {record_id} even after cleaning. Content: {text_content[:200]}...")
                                    continue
                    
                    if extracted_info:
                        break
                
                if not extracted_info:
                    missing_extracted_info_lines += 1
                    logger.warning(f"Line {line_num}: Missing extracted_info for record {record_id}. Response: {json.dumps(response_data)}")
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
        logger.error(f"Error reading inference output file from GCS: {e}")
    
    # Log detailed statistics
    processed_records = len(records)
    logger.info(f"File {file_name} processing summary:")
    logger.info(f"  Total lines: {total_lines}")
    logger.info(f"  Successfully processed records: {processed_records}")
    logger.info(f"  Skipped empty/error lines: {skipped_lines}")
    logger.info(f"  Invalid JSON lines: {invalid_json_lines}")
    logger.info(f"  Missing custom_id lines: {missing_record_id_lines}")
    logger.info(f"  Missing extracted_info lines: {missing_extracted_info_lines}")
    logger.info(f"  Truncated responses (MAX_TOKENS): {truncated_responses}")
    logger.info(f"  Total unprocessed lines: {total_lines - processed_records}")
    
    if total_lines != processed_records:
        logger.warning(f"MISMATCH: Expected {total_lines} records but only processed {processed_records} records from file {file_name}")
    
    return records

def update_firestore_records(db_client, records):
    """
    Update Firestore records with the extracted information.
    """
    updated_count = 0
    not_found_records = []
    skipped_already_updated = 0
    processing_errors = 0
    
    logger.info(f"Starting to update {len(records)} Firestore records")
    if records and len(records) > 0:
        logger.debug(f"Sample record structure: {json.dumps(records[0], indent=2)[:500]}...")
    
    collection_ref = db_client.collection(FIRESTORE_COLLECTION)
    
    for record_index, record in enumerate(records, 1):
        record_id = record['record_id']
        extracted_info = record['extracted_info']
        
        # Remove null and empty string values from extracted_info
        cleaned_info = {}
        for key, value in extracted_info.items():
            if value is not None and value != "":
                if isinstance(value, dict):
                    nested_cleaned = {k: v for k, v in value.items() if v is not None and v != ""}
                    if nested_cleaned:
                        cleaned_info[key] = nested_cleaned
                else:
                    cleaned_info[key] = value
        
        # Add isJobUpdated field with value true
        cleaned_info['isJobUpdated'] = True
        
        try:
            doc_ref = collection_ref.document(record_id)
            doc = doc_ref.get()
            
            if not doc.exists:
                logger.warning(f"Document with ID {record_id} not found in collection {FIRESTORE_COLLECTION}")
                not_found_records.append(record_id)
                continue
            
            doc_data = doc.to_dict()
            
            # Check if document already has isJobUpdated field set to true
            if doc_data.get('isJobUpdated') == True:
                skipped_already_updated += 1
                logger.info(f"Record {record_index}/{len(records)}: Skipping document {record_id} as it's already updated (isJobUpdated=true)")
                continue
            
            # Update the document
            doc_ref.update(cleaned_info)
            updated_count += 1
            logger.info(f"Record {record_index}/{len(records)}: Successfully updated document {record_id}")
            
            # Get updated document data for vector generation
            updated_doc = doc_ref.get()
            updated_doc_data = updated_doc.to_dict()
            
            # Trigger vector index generation after successful update
            trigger_vector_index_generation(record_id, updated_doc_data)
                
        except Exception as e:
            processing_errors += 1
            logger.error(f"Record {record_index}/{len(records)}: Error updating document {record_id}: {e}")
    
    # Log detailed processing summary
    total_processed = updated_count + len(not_found_records) + skipped_already_updated + processing_errors
    logger.info(f"Firestore update summary:")
    logger.info(f"  Total records to process: {len(records)}")
    logger.info(f"  Successfully updated: {updated_count}")
    logger.info(f"  Not found in Firestore: {len(not_found_records)}")
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
    AWS Lambda function handler triggered by API Gateway.
    """
    try:
        # Parse request body from API Gateway
        body = json.loads(event.get('body', '{}'))
        file_name = body.get('file_name')
        bucket_name = body.get('bucket_name')
        
        if not file_name or not bucket_name:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'file_name and bucket_name required'})
            }
        
        logger.info(f"Processing file: {file_name} from bucket: {bucket_name}")
        
        # Initialize Firestore client
        db_client = get_firestore_client()
        
        # Read records from GCS file
        records = read_inference_output_from_gcs(bucket_name, file_name)
        
        if not records:
            logger.warning(f"No valid records found in file {file_name}")
            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'No records to process'})
            }
        
        # Update Firestore records
        logger.info(f"Processing {len(records)} records from file {file_name} for update...")
        updated_count, not_found_records = update_firestore_records(db_client, records)
        
        logger.info(f"File {file_name} update summary: {updated_count} updated, {len(not_found_records)} not found")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Process completed successfully',
                'totalUpdated': updated_count,
                'totalNotFound': len(not_found_records)
            })
        }
    
    except Exception as e:
        error_msg = f"Error in Lambda execution: {str(e)}"
        logger.error(error_msg)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': error_msg})
        }
