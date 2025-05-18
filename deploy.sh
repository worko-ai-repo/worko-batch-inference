#!/bin/bash

# Install dependencies
pip install -r requirements.txt -t .

# Build the SAM application
sam build

# Deploy the SAM application
sam deploy --guided
