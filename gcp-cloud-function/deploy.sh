#!/bin/bash

# Deploy GCP Cloud Function
# Usage: ./deploy.sh [dev|prod]

GCLOUD=/opt/homebrew/share/google-cloud-sdk/bin/gcloud
ENV=${1:-dev}

if [ "$ENV" = "prod" ]; then
  GCP_PROJECT_ID="worko-prod"
  FUNCTION_NAME="vertex-ai-api-notify-prod"
  GCS_BUCKET="worko-batch-inference-file-prod"
  API_URL="https://dq31rkfmni.execute-api.ap-south-1.amazonaws.com/prod/process"
  API_KEY="REDACTED_SECRET"
else
  GCP_PROJECT_ID="worko-dev"
  FUNCTION_NAME="vertex-ai-api-notify-dev"
  GCS_BUCKET="worko-batch-inference-file-dev"
  API_URL="https://aj431p30v4.execute-api.ap-south-1.amazonaws.com/prod/process"
  API_KEY="REDACTED_SECRET"
fi

REGION="asia-south1"

echo "Deploying to $ENV environment..."

$GCLOUD functions deploy $FUNCTION_NAME \
  --runtime python311 \
  --trigger-resource $GCS_BUCKET \
  --trigger-event google.storage.object.finalize \
  --entry-point notify_api \
  --region $REGION \
  --project $GCP_PROJECT_ID \
  --memory 256MB \
  --timeout 60s \
  --max-instances 10 \
  --set-env-vars API_URL=$API_URL,API_KEY=$API_KEY

echo "Deployment complete!"
echo "Environment: $ENV"
echo "Function: $FUNCTION_NAME"
echo "Trigger: gs://$GCS_BUCKET"
