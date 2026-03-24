# GCP Cloud Function - Vertex AI API Notifier

This Cloud Function notifies a REST API when Vertex AI batch prediction output files are created in GCS.

## Setup Instructions

### 1. Update Configuration

Edit `main.py` and update:
- `API_URL` with your REST API endpoint

Edit `deploy.sh` and update:
- `GCP_PROJECT_ID`
- `GCS_BUCKET` (your Vertex AI output bucket)
- `REGION`

### 2. Deploy Function

```bash
chmod +x deploy.sh
./deploy.sh
```

## How It Works

1. Vertex AI writes prediction output to GCS bucket
2. GCS triggers Cloud Function on file creation
3. Cloud Function calls REST API with file metadata

## Testing

```bash
# Upload a test file to trigger the function
gsutil cp test-file.jsonl.out gs://YOUR_BUCKET/output/bright-data-external-jobs/
```

## Monitoring

```bash
# View logs
gcloud functions logs read vertex-ai-api-notify-dev --limit 50
```
