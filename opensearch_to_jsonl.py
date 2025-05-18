#!/usr/bin/env python3
"""
Script to extract job records from OpenSearch and create JSONL files.

This script connects to OpenSearch, queries for records with 'externalJobId' present,
and creates a file.jsonl for each record with the 'jobDescription' field value in the 'text' field,
prepending a {prompt} placeholder.
"""

import os
import json
import logging
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import boto3

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# OpenSearch connection parameters
# These should match the environment variables used in the worko-job-app
OPENSEARCH_ENDPOINT = os.environ.get('APP_OPENSEARCH_ENDPOINT')
OPENSEARCH_INDEX = 'worko-jobs'
REGION = os.environ.get('APP_SLS_REGION', 'us-east-1')

# Local development settings
IS_LOCAL = os.environ.get('APP_STAGE') == 'local'
OPENSEARCH_USERNAME = os.environ.get('APP_OPENSEARCH_DASHBOARD_USER_NAME')
OPENSEARCH_PASSWORD = os.environ.get('APP_OPENSEARCH_DASHBOARD_PASSWORD')

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
            verify_certs=True,
            connection_class=RequestsHttpConnection
        )
    
    return client

def query_jobs_with_external_id(client):
    """
    Query OpenSearch for jobs that have an externalJobId field.
    
    Args:
        client: OpenSearch client
        
    Returns:
        List of job records
    """
    query = {
        "query": {
            "exists": {
                "field": "externalJobId"
            }
        },
        "size": 1000  # Adjust based on expected number of records
    }
    
    try:
        response = client.search(
            body=query,
            index=OPENSEARCH_INDEX
        )
        
        hits = response.get('hits', {}).get('hits', [])
        logger.info(f"Found {len(hits)} jobs with externalJobId")
        return hits
    except Exception as e:
        logger.error(f"Error querying OpenSearch: {e}")
        return []

def create_jsonl_files(jobs):
    """
    Create JSONL files for each job record.
    
    Args:
        jobs: List of job records from OpenSearch
    """
    output_dir = os.path.join(os.getcwd(), 'output')
    os.makedirs(output_dir, exist_ok=True)
    
    for job in jobs:
        job_source = job.get('_source', {})
        external_job_id = job_source.get('externalJobId')
        job_description = job_source.get('jobDescription', '')
        
        if not external_job_id or not job_description:
            logger.warning(f"Skipping job with missing data: {job_source.get('id')}")
            continue
        
        # Create the JSONL entry with {prompt} placeholder prepended to job description
        jsonl_entry = {
            "text": f"{{prompt}} {job_description}"
        }
        
        # Create a file for this job
        filename = f"{external_job_id}.jsonl"
        file_path = os.path.join(output_dir, filename)
        
        try:
            with open(file_path, 'w') as f:
                f.write(json.dumps(jsonl_entry) + '\n')
            logger.info(f"Created file: {file_path}")
        except Exception as e:
            logger.error(f"Error creating file {filename}: {e}")

def main():
    """Main function to execute the script."""
    if not OPENSEARCH_ENDPOINT:
        logger.error("OpenSearch endpoint not configured. Please set APP_OPENSEARCH_ENDPOINT environment variable.")
        return
    
    logger.info("Connecting to OpenSearch...")
    client = get_opensearch_client()
    
    logger.info("Querying jobs with externalJobId...")
    jobs = query_jobs_with_external_id(client)
    
    if jobs:
        logger.info("Creating JSONL files...")
        create_jsonl_files(jobs)
        logger.info("Process completed successfully.")
    else:
        logger.info("No jobs found with externalJobId.")

if __name__ == "__main__":
    main()
