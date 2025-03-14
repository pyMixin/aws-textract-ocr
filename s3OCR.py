import json
import time
import boto3
import os



# NOT RECOMMENDED
textract_client = boto3.client(
    'textract',
    region_name='us-east-1',
       aws_access_key_id='xxxx',
       aws_secret_access_key='xxxx'
)

s3_client = boto3.client('s3') 


OUTPUT_BUCKET = "ocr-func-output"



def lambda_handler(event, context):

    
    print("## EVENT RECEIVED ##")
    print(json.dumps(event))

    # 1) Lambda function region
    lambda_region = os.environ.get('AWS_REGION', 'Unknown')
    print(f"Lambda running in region: {lambda_region}")

    # 2) Textract client region
    textract_client_region = textract_client.meta.region_name
    print(f"Textract client region: {textract_client_region}")

    # 3) S3 client region
    s3_client_region = s3_client.meta.region_name
    print(f"S3 client region: {s3_client_region}")


    record = event['Records'][0]
    bucket_name = record['s3']['bucket']['name']
    key_name = record['s3']['object']['key']

    print(f"Bucket Name: {bucket_name}")
    print(f"Object Key: {key_name}")
    
    # Extract the filename from the key
    full_name = key_name.split('/')[-1]  # e.g. "document.pdf"
    name_without_ext = full_name.split('.')[0]  # e.g. "document"
    
    # Start Textract job
    print("Starting Textract job...")
    response = textract_client.start_document_text_detection(
        DocumentLocation={
            'S3Object': {
                'Bucket': bucket_name,
                'Name': key_name
            }
        }
    )
    print("Textract job started:", response)
    
    # Wait for completion (inefficient approach)
    time.sleep(120)  # WARNING: your Lambda timeout must be >= 3 minutes
    
    job_id = response['JobId']
    print("Textract Job ID:", job_id)
    
    # Get text detection results
    response2 = textract_client.get_document_text_detection(JobId=job_id)
    
    print("Textract detection response:", response2)
    
    blocks = response2.get('Blocks', [])
    s_line = ''
    s_word = ''

    print("----- Recognized Lines -----")
    
    for block in blocks:
        if block['BlockType'] == 'LINE':
            s_line += block['Text'] + ';'
        elif block['BlockType'] == 'WORD':
            s_word += block['Text'] + ';'
    


    full_name = key_name.split('/')[-1]
    name_without_ext = full_name.split('.')[0]
    key_line = name_without_ext + "_linewise.txt"
    key_word = name_without_ext + "_wordwise.txt"
    
    # Put the text files back into the same bucket (different prefix)
    s3_client.put_object(Body=s_line, Bucket=OUTPUT_BUCKET, Key=key_line)
    s3_client.put_object(Body=s_word, Bucket=OUTPUT_BUCKET, Key=key_word)

    print(f"Uploaded linewise text to s3://{bucket_name}/{line_key}")
    print(f"Uploaded wordwise text to s3://{bucket_name}/{word_key}")

    
    return {
        'statusCode': 200,
        'body': json.dumps('Successfully processed document with Amazon Textract.')
    }

