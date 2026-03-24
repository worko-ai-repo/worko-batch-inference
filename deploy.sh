#!/bin/bash

set -e

ENV=${1:-dev}

if [ "$ENV" != "dev" ] && [ "$ENV" != "prod" ]; then
    echo "Usage: ./deploy.sh [dev|prod]"
    exit 1
fi

PROFILE=""
if [ "$ENV" = "prod" ]; then
    PROFILE="--profile worko-p"
fi

echo "Building SAM application for $ENV environment..."
sam build --use-container --skip-pull-image

echo "Deploying to AWS ($ENV)..."
sam deploy $PROFILE --resolve-image-repos --capabilities CAPABILITY_IAM --parameter-overrides Environment=$ENV

echo "Deployment complete!"
