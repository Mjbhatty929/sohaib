import boto3
import os
from io import BytesIO
from PIL import Image

s3 = boto3.client("s3")

SOURCE_BUCKET = "s3-source-bucket"
TARGET_BUCKET = "s3-target-bucket65"  # Make sure this bucket name is correct

def lambda_handler(event, context):
    # Extract bucket and object key from event
    record = event["Records"][0]
    source_bucket = record["s3"]["bucket"]["name"]
    object_key = record["s3"]["object"]["key"]

    if not object_key.lower().endswith(".jpg"):
        print(f"Skipping non-JPG file: {object_key}")
        return

    try:
        # Download JPG from S3
        response = s3.get_object(Bucket=source_bucket, Key=object_key)
        image_data = response["Body"].read()

        # Convert image to PNG
        img = Image.open(BytesIO(image_data))
        img = img.convert("PNG")  # Ensure correct mode

        # Save PNG to in-memory buffer
        output_buffer = BytesIO()
        img.save(output_buffer, format="PNG")
        output_buffer.seek(0)

        # Construct new filename with .png extension
        new_key = os.path.splitext(object_key)[0] + ".png"

        # Upload PNG to target S3 bucket
        s3.put_object(Bucket=TARGET_BUCKET, Key=new_key, Body=output_buffer, ContentType="image/png")

        print(f"Successfully converted {object_key} to {new_key} and saved to {TARGET_BUCKET}")
    
    except Exception as e:
        print(f"Error processing {object_key}: {str(e)}")
        raise
