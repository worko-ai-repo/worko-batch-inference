# AWS Lambda + API Gateway Setup

## 1. Create Lambda Function

```bash
# Package dependencies
pip install google-cloud-storage google-cloud-firestore -t package/
cp lambda_handler.py package/
cd package && zip -r ../lambda_function.zip . && cd ..

# Create Lambda function
aws lambda create-function \
  --function-name gcs-firestore-processor \
  --runtime python3.11 \
  --role arn:aws:iam::YOUR_ACCOUNT_ID:role/lambda-execution-role \
  --handler lambda_handler.lambda_handler \
  --zip-file fileb://lambda_function.zip \
  --timeout 300 \
  --memory-size 512
```

## 2. Create API Gateway

```bash
# Create REST API
aws apigateway create-rest-api --name gcs-processor-api

# Get API ID and root resource ID
API_ID=<your-api-id>
ROOT_ID=<your-root-resource-id>

# Create resource
aws apigateway create-resource \
  --rest-api-id $API_ID \
  --parent-id $ROOT_ID \
  --path-part process

# Create POST method
RESOURCE_ID=<your-resource-id>
aws apigateway put-method \
  --rest-api-id $API_ID \
  --resource-id $RESOURCE_ID \
  --http-method POST \
  --authorization-type NONE

# Integrate with Lambda
aws apigateway put-integration \
  --rest-api-id $API_ID \
  --resource-id $RESOURCE_ID \
  --http-method POST \
  --type AWS_PROXY \
  --integration-http-method POST \
  --uri arn:aws:apigateway:REGION:lambda:path/2015-03-31/functions/arn:aws:lambda:REGION:ACCOUNT_ID:function:gcs-firestore-processor/invocations

# Deploy API
aws apigateway create-deployment \
  --rest-api-id $API_ID \
  --stage-name prod
```

## 3. API Endpoint

Your API endpoint will be:
```
https://{API_ID}.execute-api.{REGION}.amazonaws.com/prod/process
```

## 4. Update GCP Cloud Function

Update `main.py` with your API endpoint:
```python
API_URL = "https://{API_ID}.execute-api.{REGION}.amazonaws.com/prod/process"
```

## 5. Test

```bash
curl -X POST https://YOUR_API_ENDPOINT/process \
  -H "Content-Type: application/json" \
  -d '{"file_name": "output/bright-data-external-jobs/test.jsonl.out", "bucket_name": "worko-batch-inference-dev"}'
```
