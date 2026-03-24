"""
GCP Cloud Function to notify REST API when Vertex AI batch prediction completes.
Triggers when a .jsonl.out file is created in GCS bucket.
"""
import os
import requests

API_URL = os.environ.get('API_URL')
API_KEY = os.environ.get('API_KEY')

def notify_api(event, context):
    """
    Triggered by GCS file creation.
    Calls REST API with file metadata.
    """
    print(f"Function triggered! Event: {event}")
    
    file_name = event['name']
    bucket_name = event['bucket']
    
    if not file_name.startswith('output/bright-data-external-jobs/'):
        print(f"Skipping file not in target path: {file_name}")
        return
    
    if not file_name.endswith('.jsonl.out') and not file_name.endswith('.jsonl'):
        print(f"Skipping non-prediction file: {file_name}")
        return
    
    print(f"Processing file: {file_name} from bucket: {bucket_name}")
    
    try:
        print(f"Calling API: {API_URL}")
        print(f"Payload: file_name={file_name}, bucket_name={bucket_name}")
        
        response = requests.post(
            API_URL,
            json={
                'file_name': file_name,
                'bucket_name': bucket_name
            },
            headers={'x-api-key': API_KEY},
            timeout=30
        )
        
        print(f"Response status: {response.status_code}")
        print(f"Response body: {response.text}")
        
        response.raise_for_status()
        
        print(f"Successfully notified API: {response.status_code}")
        
    except Exception as e:
        print(f"Error processing file {file_name}: {str(e)}")
        raise
