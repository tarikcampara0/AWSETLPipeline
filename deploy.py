
import argparse
import json
import os
import zipfile
import boto3
from pathlib import Path


LAMBDA_HANDLER = '''
import json, boto3, pandas as pd, io

s3 = boto3.client("s3")

def handler(event, context):
    record   = event["Records"][0]["s3"]
    bucket   = record["bucket"]["name"]
    key      = record["object"]["key"]
    dest_bucket = os.environ["DEST_BUCKET"]

    obj = s3.get_object(Bucket=bucket, Key=key)
    df  = pd.read_csv(io.BytesIO(obj["Body"].read()), parse_dates=["timestamp"])

    df = df.drop_duplicates(subset=["transaction_id"])
    df = df[df["status"] == "completed"]
    df["net_revenue"] = (df["total"] * (1 - df["discount_pct"] / 100)).round(2)
    df["date"]        = df["timestamp"].dt.date.astype(str)

    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)

    out_key = key.replace(".csv", "_transformed.parquet").replace("landing/", "")
    s3.put_object(Bucket=dest_bucket, Key=out_key, Body=buf.getvalue())

    return {"statusCode": 200, "output_key": out_key, "rows": len(df)}
'''
def create_lambda_zip():
    zip_path = Path("/tmp/etl_lambda.zip")
    with zipfile.ZipFile(zip_path, "w") as z:
        z.writestr("lambda_function.py", LAMBDA_HANDLER_CODE)
    return zip_path


def deploy(landing_bucket: str, processed_bucket: str, region: str):
    session    = boto3.Session(region_name=region)
    s3_client  = session.client("s3")
    lam_client = session.client("lambda")
    iam_client = session.client("iam")
    for bucket in [landing_bucket, processed_bucket]:
        try:
            if region == "us-east-1":
                s3_client.create_bucket(Bucket=bucket)
            else:
                s3_client.create_bucket(
                    Bucket=bucket,
                    CreateBucketConfiguration={"LocationConstraint": region}
                )
            print(f"Created bucket: {bucket}")
        except s3_client.exceptions.BucketAlreadyOwnedByYou:
            print(f"Bucket exists: {bucket}")

    # Create IAM role for Lambda
    trust_policy = json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action":  "sts:AssumeRole"
                }]
    })
    try:
        role = iam_client.create_role(
            RoleName="ETLLambdaRole",
            AssumeRolePolicyDocument=trust_policy,
        )
        role_arn = role["Role"]["Arn"]
        iam_client.attach_role_policy(
            RoleName="ETLLambdaRole",
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3FullAccess"
        )
        iam_client.attach_role_policy(
            RoleName="ETLLambdaRole",
            PolicyArn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
        )
        print(f"Created IAM role: {role_arn}")
    except iam_client.exceptions.EntityAlreadyExistsException:
        role_arn = iam_client.get_role(RoleName="ETLLambdaRole")["Role"]["Arn"]
        print(f"IAM role exists: {role_arn}")

    import time; time.sleep(10)  #role propagation