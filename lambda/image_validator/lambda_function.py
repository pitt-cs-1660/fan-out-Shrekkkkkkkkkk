import json
import os
import boto3
from urllib.parse import unquote_plus

s3 = boto3.client('s3')

VALID_EXTENSIONS = ['.jpg', '.jpeg', '.png', '.gif']

def is_valid_image(key):
    """check if the file has a valid image extension."""
    _, ext = os.path.splitext(key.lower())
    return ext in VALID_EXTENSIONS

def lambda_handler(event, context):
    """
    validates that uploaded files are images.
    raises exception for invalid files (triggers DLQ).
    """

    print("=== image validator invoked ===")

    for record in event['Records']:
        sns_message = record['Sns']['Message']
        s3_event = json.loads(sns_message)

        for s3_record in s3_event['Records']:
            bucket = s3_record['s3']['bucket']['name']
            key = unquote_plus(s3_record['s3']['object']['key'])

            if is_valid_image(key):
                print(f"[VALID] {key} is a valid image file")

                filename = key.split('/')[-1]

                s3.copy_object(
                    Bucket=bucket,
                    Key=f"processed/valid/{filename}",
                    CopySource={'Bucket': bucket, 'Key': key}
                )
            else:
                print(f"[INVALID] {key} is not a valid image type")
                raise ValueError(f"{key} is not a valid image type")

    return {'statusCode': 200, 'body': 'validation complete'}
