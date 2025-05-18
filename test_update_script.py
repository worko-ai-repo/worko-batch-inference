#!/usr/bin/env python3
"""
Test script to verify the functionality of update_opensearch_from_inference.py
without requiring an actual OpenSearch connection.
"""

import json
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# File paths
INFERENCE_OUTPUT_FILE = 'file-1.jsonl.out'
TEST_OUTPUT_FILE = 'test_updates.json'

def read_inference_output(file_path):
    """
    Read the Bedrock batch inference output file and extract document IDs and processed data.
    
    Args:
        file_path: Path to the inference output file
        
    Returns:
        List of dictionaries containing document ID and extracted information
    """
    records = []
    
    try:
        with open(file_path, 'r') as f:
            for i, line in enumerate(f):
                if i >= 10:  # Just process the first 10 records for testing
                    break
                    
                try:
                    data = json.loads(line)
                    record_id = data.get('recordId')
                    
                    # Extract the model output which contains the processed information
                    model_output = data.get('modelOutput', {})
                    content = model_output.get('content', [])
                    
                    # Extract the JSON from the text content
                    extracted_info = None
                    for item in content:
                        if item.get('type') == 'text':
                            try:
                                extracted_info = json.loads(item.get('text', '{}'))
                                break
                            except json.JSONDecodeError:
                                logger.warning(f"Could not parse JSON from content for record {record_id}")
                    
                    if record_id and extracted_info:
                        records.append({
                            'record_id': record_id,
                            'extracted_info': extracted_info
                        })
                    else:
                        logger.warning(f"Missing record_id or extracted_info in line: {line[:100]}...")
                        
                except json.JSONDecodeError:
                    logger.error(f"Invalid JSON in line: {line[:100]}...")
                except Exception as e:
                    logger.error(f"Error processing line: {e}")
    
    except Exception as e:
        logger.error(f"Error reading inference output file: {e}")
    
    logger.info(f"Processed {len(records)} records from inference output")
    return records

def simulate_opensearch_update(records):
    """
    Simulate updating OpenSearch records by writing to a test output file.
    
    Args:
        records: List of records with document IDs and extracted information
    """
    updates = []
    
    for record in records:
        record_id = record['record_id']
        extracted_info = record['extracted_info']
        
        # Simulate an update operation
        update = {
            'index': 'worko-jobs',
            'id': record_id,
            'update': {
                'extractedInfo': extracted_info
            }
        }
        updates.append(update)
        logger.info(f"Simulated update for document {record_id}")
    
    # Write the updates to a file
    try:
        with open(TEST_OUTPUT_FILE, 'w') as f:
            json.dump(updates, f, indent=2)
        logger.info(f"Wrote {len(updates)} simulated updates to {TEST_OUTPUT_FILE}")
    except Exception as e:
        logger.error(f"Error writing to test output file: {e}")

def main():
    """Main function to execute the test script."""
    logger.info(f"Reading inference output from {INFERENCE_OUTPUT_FILE}...")
    records = read_inference_output(INFERENCE_OUTPUT_FILE)
    
    if records:
        logger.info("Simulating OpenSearch updates...")
        simulate_opensearch_update(records)
        logger.info("Test completed successfully.")
    else:
        logger.info("No records found in inference output.")

if __name__ == "__main__":
    main()
