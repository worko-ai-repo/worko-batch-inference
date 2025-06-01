# Worko Batch Inference OpenSearch Updater

This project contains a Lambda function that updates OpenSearch records with processed data from Bedrock batch inference.

## Overview

The Lambda function:
1. Reads `.jsonl.out` files from an S3 bucket
2. Extracts document IDs and processed information
3. Updates corresponding records in OpenSearch
4. Logs records that aren't found in OpenSearch

## Features

- **S3 Integration**: Reads files directly from S3
- **Date and Extension Filtering**: Only processes files created today with the `.jsonl.out` extension
- **Direct Field Updates**: Updates fields directly in the document
- **Null Value Removal**: Removes null values from the data before updating OpenSearch
- **Exponential Backoff**: Retries API calls with exponential backoff when throttled

## Deployment

### Prerequisites

- AWS CLI installed and configured
- AWS SAM CLI installed
- Python 3.9 or later

### Deployment Steps

1. Clone this repository
2. Navigate to the project directory
3. Run the deployment script:

```bash
./deploy.sh
```

During the guided deployment, you'll be prompted to provide values for parameters like OpenSearch endpoint, S3 bucket name, etc.

## Project Structure

- `lambda_function.py`: The Lambda function code
- `requirements.txt`: Python dependencies
- `template.yaml`: SAM template for deploying the Lambda function
- `deploy.sh`: Deployment script
