import json
import os
import urllib.parse
import uuid

import boto3

# enhanced version of the rekognition handler from lecture 6. triggered by S3
# ObjectCreated events (from the myauto_to_s3.py uploads), runs detect_labels
# on each image, and stores the result in DynamoDB table 'rekogintionAnalysesDB'.


def get_image_labels(bucket, key):
    rekognition_client = boto3.client("rekognition")
    response = rekognition_client.detect_labels(
        Image={"S3Object": {"Bucket": bucket, "Name": key}},
        MaxLabels=10,
    )
    return response


def make_item(data):
    # DynamoDB doesn't accept native floats, everything has to be Decimal or
    # string. easiest fix here is converting floats to strings recursively,
    # same approach as the lecture script.
    if isinstance(data, dict):
        return {k: make_item(v) for k, v in data.items()}
    if isinstance(data, list):
        return [make_item(v) for v in data]
    if isinstance(data, float):
        return str(data)
    return data


def put_labels_in_db(data, media_name, media_bucket):
    data.pop("ResponseMetadata", None)
    data["mediaType"] = "Image"
    data["mediaName"] = media_name
    data["mediaBucket"] = media_bucket
    data["id"] = str(uuid.uuid1())

    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    dynamodb = boto3.resource("dynamodb", region_name=region) if region else boto3.resource("dynamodb")
    table_name = os.environ.get("DYNAMO_DB_TABLE", "rekogintionAnalysesDB")
    table = dynamodb.Table(table_name)

    data = make_item(data)
    table.put_item(Item=data)
    return data


def start_processing_media(event, context):
    # S3 trigger entrypoint. for each uploaded object that looks like an
    # image, send it to Rekognition and store the labels in DynamoDB.
    results = []
    for record in event.get("Records", []):
        try:
            object_key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
            bucket_name = record["s3"]["bucket"]["name"]
        except KeyError as e:
            print(f"skipping malformed record, missing key: {e}")
            continue

        extension = object_key.rsplit(".", 1)[-1].lower() if "." in object_key else ""
        if extension not in ("jpeg", "jpg", "png"):
            print(f"skipping '{object_key}', not an image extension we handle ({extension})")
            continue

        try:
            labels_response = get_image_labels(bucket_name, object_key)
        except Exception as e:
            # don't let one bad image kill the whole batch - S3 can send multiple
            # records in a single event
            print(f"rekognition failed for s3://{bucket_name}/{object_key}: {e}")
            continue

        try:
            saved_item = put_labels_in_db(labels_response, object_key, bucket_name)
            print(f"stored labels for s3://{bucket_name}/{object_key} as id={saved_item.get('id')}")
            results.append(saved_item.get("id"))
        except Exception as e:
            print(f"failed to write to DynamoDB for s3://{bucket_name}/{object_key}: {e}")
            continue

    return {
        "statusCode": 200,
        "body": json.dumps({"processed": len(results), "ids": results}),
    }
