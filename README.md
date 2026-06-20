# myauto -> S3 -> Rekognition -> DynamoDB pipeline

Scrapes car images from myauto.ge, uploads them to S3, and runs every uploaded
image through AWS Rekognition automatically (via an S3-triggered Lambda),
storing the label results in DynamoDB.

## structure

```
cli/
  myauto_to_s3.py     - scrapes myauto.ge, recursively uploads images to S3
  requirements.txt    - aiohttp + boto3 for the CLI

lambda/
  handler.py          - S3-triggered Lambda, runs Rekognition, writes to DynamoDB

diagram/
  architecture.drawio - open in draw.io / diagrams.net

serverless.yml         - deploys S3 bucket + Lambda + DynamoDB table together
requirements.txt       - lambda dependencies (boto3 ships with the runtime
                          anyway, this is just here for completeness)
```

## how it fits together

1. `myauto_to_s3.py` scrapes N pages of myauto.ge listings, downloads the car
   photos locally, then recursively uploads everything to an S3 bucket
   (creates the bucket if it doesn't already exist).
2. Every new object landing in that bucket fires an S3 `ObjectCreated` event.
3. The Lambda (`handler.py`) picks up the event, calls
   `rekognition.detect_labels()` on the image, and writes the result into a
   DynamoDB table called `rekogintionAnalysesDB`.

## running the CLI

```bash
pip install -r cli/requirements.txt
python3 cli/myauto_to_s3.py --pages 2 --bucket-name your-bucket-name
```

useful flags:
- `--zip` - also zip the downloaded images locally
- `--s3-prefix cars/` - upload under a folder/prefix in the bucket
- `--skip-upload` - just scrape and download, skip S3 entirely (for testing)
- `--region eu-central-1` - change the bucket's region

run `python3 cli/myauto_to_s3.py --help` for the full list.

## deploying the lambda + dynamodb table

bonus path, using the Serverless Framework:

```bash
npm install -g serverless
serverless deploy
```

this provisions the S3 bucket, the Lambda with its S3 trigger, the
`rekogintionAnalysesDB` DynamoDB table, and the IAM permissions the Lambda
needs (s3:GetObject, rekognition:DetectLabels, dynamodb:PutItem).

note: the bucket name in `serverless.yml` (`custom.bucketName`) needs to be
globally unique - it defaults to including your AWS account id to help with
that, but double check it doesn't collide before deploying. if you deploy the
bucket this way, point `myauto_to_s3.py --bucket-name` at the same bucket name
so uploads actually land where the Lambda is watching.

if you'd rather wire the Lambda up manually through the console/CLI instead of
Serverless Framework, you just need: an S3 trigger on `ObjectCreated:*`
pointing at `handler.start_processing_media`, and an execution role with the
three permissions listed above.
