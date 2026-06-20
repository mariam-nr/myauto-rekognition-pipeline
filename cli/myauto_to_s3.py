#!/usr/bin/env python3
# myauto_to_s3.py
#
# enhanced version of the myauto scraper from lecture 8. scrapes car images from
# myauto.ge and recursively uploads everything to an S3 bucket (creates the
# bucket first if it doesn't exist yet). this is what feeds the S3 -> Lambda ->
# Rekognition -> DynamoDB pipeline.
#
# examples:
#   python3 myauto_to_s3.py --pages 2 --bucket-name my-myauto-images-bucket
#   python3 myauto_to_s3.py --pages 1 --bucket-name my-bucket --s3-prefix cars/ --zip
#   python3 myauto_to_s3.py --pages 1 --bucket-name my-bucket --region eu-central-1

import argparse
import asyncio
import os
import sys
import zipfile

import aiohttp
import boto3
from botocore.exceptions import ClientError, BotoCoreError, EndpointConnectionError


def log(msg):
    print(f"[*] {msg}")


def fail(msg, exit_code=1):
    print(f"[!] ERROR: {msg}", file=sys.stderr)
    sys.exit(exit_code)


def auto_page_n(nth_page):
    return (f"https://api2.myauto.ge/ka/products?TypeID=0&ForRent=&Mans=&CurrencyID=3&MileageType=1&Page={nth_page}")


headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "Accept-Language": "en-US,en;q=0.9",
}


async def download_image(session, url, save_directory, semaphore):
    # semaphore caps how many downloads run at once so we don't hammer the site
    # or open hundreds of file handles at the same time
    async with semaphore:
        try:
            async with session.get(url) as response:
                response.raise_for_status()
                filename = os.path.basename(url)
                save_path = os.path.join(save_directory, filename)
                with open(save_path, "wb") as file:
                    while True:
                        chunk = await response.content.read(1024)
                        if not chunk:
                            break
                        file.write(chunk)
                print(f"Downloaded: {filename}")
        except aiohttp.ClientError as e:
            print(f"Error downloading image: {e}")


def ensure_bucket(s3_client, bucket_name, region):
    # check if the bucket exists, create it if not
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        log(f"bucket '{bucket_name}' already exists, using it")
        return
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        # head_bucket gives back 404 (NotFound) or sometimes plain 403 depending on
        # permissions, but a 403 here could also mean someone else owns the bucket
        # name globally - we still try to create it and let AWS tell us the real reason
        if code not in ("404", "NoSuchBucket"):
            log(f"head_bucket returned {code}, will try to create the bucket anyway")

    try:
        if region == "us-east-1":
            # us-east-1 is the one region where you must NOT pass a LocationConstraint
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
        log(f"bucket '{bucket_name}' created in {region}")
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code == "BucketAlreadyOwnedByYou":
            log(f"bucket '{bucket_name}' already exists and is owned by you, continuing")
        elif code == "BucketAlreadyExists":
            fail(f"bucket name '{bucket_name}' is already taken by someone else (S3 names "
                 f"are globally unique). pick a different --bucket-name.")
        elif code in ("AccessDenied", "UnauthorizedOperation"):
            fail(f"permission denied creating bucket ({code}). need s3:CreateBucket.")
        else:
            fail(f"AWS error creating bucket: {code} - {e.response.get('Error', {}).get('Message', str(e))}")


def upload_to_s3(s3_client, local_directory, bucket_name, s3_prefix=""):
    # walks the local dir recursively and uploads every file, keeping the folder
    # structure under s3_prefix
    uploaded = 0
    failed = 0
    for root, _, files in os.walk(local_directory):
        for file in files:
            local_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_path, local_directory)
            s3_key = os.path.join(s3_prefix, relative_path).replace("\\", "/")

            try:
                s3_client.upload_file(local_path, bucket_name, s3_key)
                print(f"Uploaded {local_path} to s3://{bucket_name}/{s3_key}")
                uploaded += 1
            except ClientError as e:
                code = e.response.get("Error", {}).get("Code", "")
                print(f"Error uploading {local_path}: {code} - "
                      f"{e.response.get('Error', {}).get('Message', str(e))}")
                failed += 1

    log(f"upload done: {uploaded} succeeded, {failed} failed")
    return uploaded, failed


async def scrape_images(args):
    image_urls = []

    async with aiohttp.ClientSession(headers=headers) as session:
        for page_n in range(args.pages):
            try:
                response = await session.get(auto_page_n(page_n))
                response.raise_for_status()
                data = await response.json()
            except aiohttp.ClientError as e:
                print(f"Error fetching page {page_n}: {e}")
                continue

            items = data.get("data", {}).get("items", [])
            for item in items:
                car_id = item["car_id"]
                photo = item["photo"]
                picn = item["pic_number"]
                print(f"Car ID: {car_id}")
                print("Image URLs:")
                for pic_id in range(1, picn + 1):
                    image_url = f"https://static.my.ge/myauto/photos/{photo}/large/{car_id}_{pic_id}.jpg"
                    image_urls.append(image_url)
                    print(image_url)
                print()

    save_directory = args.output_dir
    os.makedirs(save_directory, exist_ok=True)

    semaphore = asyncio.Semaphore(args.concurrency)
    tasks = []
    async with aiohttp.ClientSession() as session:
        for url in image_urls:
            task = asyncio.ensure_future(download_image(session, url, save_directory, semaphore))
            tasks.append(task)
        await asyncio.gather(*tasks)

    if args.zip:
        zip_filename = f"{save_directory}.zip"
        with zipfile.ZipFile(zip_filename, "w") as zip_file:
            for root, _, files in os.walk(save_directory):
                for file in files:
                    file_path = os.path.join(root, file)
                    zip_file.write(file_path, arcname=file)
        print(f"\nAll images downloaded and zipped successfully.")
        zip_file_size_mb = os.path.getsize(zip_filename) / (1024 * 1024)
        print(f"ZIP file size: {zip_file_size_mb:.2f} MB")

    total_images = sum(len(files) for _, _, files in os.walk(save_directory))
    print(f"Total number of downloaded images: {total_images}")

    return save_directory, total_images


def parse_args():
    parser = argparse.ArgumentParser(
        description="Scrape car images from myauto.ge and recursively upload them to S3.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--pages", type=int, default=1, help="number of myauto.ge pages to scrape")
    parser.add_argument("--output-dir", type=str, default="downloaded_images",
                         help="local directory to save images before upload")
    parser.add_argument("--concurrency", type=int, default=20,
                         help="max number of images to download at the same time")
    parser.add_argument("--zip", action="store_true", help="also create a local zip of downloaded images")

    parser.add_argument("--bucket-name", required=True, help="S3 bucket to upload images to (created if missing)")
    parser.add_argument("--region", default="us-east-1", help="AWS region for the bucket")
    parser.add_argument("--s3-prefix", type=str, default="",
                         help="S3 key prefix/folder to upload into, e.g. 'cars/'")
    parser.add_argument("--skip-upload", action="store_true",
                         help="only scrape/download, don't upload to S3 (useful for testing the scraper alone)")

    return parser.parse_args()


def main():
    args = parse_args()

    save_directory, total_images = asyncio.run(scrape_images(args))

    if total_images == 0:
        log("no images were downloaded, nothing to upload")
        return

    if args.skip_upload:
        log("--skip-upload was set, not touching S3")
        return

    try:
        s3_client = boto3.client("s3", region_name=args.region)
    except (BotoCoreError, ClientError) as e:
        fail(f"couldn't create boto3 s3 client: {e}")

    try:
        s3_client.list_buckets()
    except EndpointConnectionError as e:
        fail(f"couldn't reach AWS for region '{args.region}': {e}")
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("AuthFailure", "UnrecognizedClientException", "InvalidClientTokenId"):
            fail(f"AWS auth failed ({code}). check your credentials / session token.")
        elif code in ("UnauthorizedOperation", "AccessDenied"):
            fail(f"credentials work but don't have permission for basic S3 calls ({code}).")

    ensure_bucket(s3_client, args.bucket_name, args.region)
    upload_to_s3(s3_client, save_directory, args.bucket_name, args.s3_prefix)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        fail("interrupted", exit_code=130)
