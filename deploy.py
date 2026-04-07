
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
