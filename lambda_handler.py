import json
import logging
import re
import boto3
from google.cloud import storage
from google.cloud import firestore
from google.oauth2 import service_account

logger = logging.getLogger()
logger.setLevel(logging.INFO)

GCP_SECRET_NAME = "GCP-firestore-secret-dev"
GCP_PROJECT_ID = "worko-dev"
FIRESTORE_DB_NAME = "worko-jobs-db-dev"
FIRESTORE_COLLECTION = "workoJobs"

def get_gcp_credentials():
    secrets_client = boto3.client('secretsmanager', region_name='ap-south-1')
    response = secrets_client.get_secret_value(SecretId=GCP_SECRET_NAME)
    credentials_dict = json.loads(response['SecretString'])
    return service_account.Credentials.from_service_account_info(credentials_dict)

def read_file_from_gcs(bucket_name, file_name):
    credentials = get_gcp_credentials()
    storage_client = storage.Client(credentials=credentials)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    content = blob.download_as_text()
    
    records = []
    for line in content.splitlines():
        if not line.strip():
            continue
        
        try:
            data = json.loads(line)
            record_id = data.get('custom_id')
            if not record_id:
                continue
            
            response_data = data.get('response', {})
            candidates = response_data.get('candidates', [])
            
            extracted_info = None
            for candidate in candidates:
                parts = candidate.get('content', {}).get('parts', [])
                for part in parts:
                    text = part.get('text', '{}')
                    if text:
                        try:
                            extracted_info = json.loads(text)
                            break
                        except json.JSONDecodeError:
                            try:
                                cleaned = re.sub(r'//[^\n]*\n?', '', text)
                                extracted_info = json.loads(cleaned)
                                break
                            except:
                                continue
                if extracted_info:
                    break
            
            if extracted_info:
                records.append({'record_id': record_id, 'extracted_info': extracted_info})
        except:
            continue
    
    return records

def update_firestore(records):
    credentials = get_gcp_credentials()
    db_client = firestore.Client(project=GCP_PROJECT_ID, database=FIRESTORE_DB_NAME, credentials=credentials)
    collection_ref = db_client.collection(FIRESTORE_COLLECTION)
    
    updated = 0
    for record in records:
        record_id = record['record_id']
        extracted_info = record['extracted_info']
        
        cleaned_info = {k: v for k, v in extracted_info.items() if v is not None and v != ""}
        cleaned_info['isJobUpdated'] = True
        
        try:
            doc_ref = collection_ref.document(record_id)
            if doc_ref.get().exists:
                doc_ref.update(cleaned_info)
                updated += 1
        except Exception as e:
            logger.error(f"Error updating {record_id}: {e}")
    
    return updated

def lambda_handler(event, context):
    try:
        body = json.loads(event.get('body', '{}'))
        file_name = body.get('file_name')
        bucket_name = body.get('bucket_name')
        
        if not file_name or not bucket_name:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'file_name and bucket_name required'})
            }
        
        logger.info(f"Processing file: {file_name} from bucket: {bucket_name}")
        
        records = read_file_from_gcs(bucket_name, file_name)
        updated = update_firestore(records)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Success',
                'totalUpdated': updated,
                'totalRecords': len(records)
            })
        }
    
    except Exception as e:
        logger.error(f"Error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
