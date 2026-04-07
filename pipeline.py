
import pandas as pd
import numpy as np
import json
import os
import shutil
import hashlib
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("etl")

# Mock S3 Buckets (local directories) 
BUCKET_LANDING   = Path("mock_s3/landing")
BUCKET_PROCESSED = Path("mock_s3/processed")
BUCKET_ARCHIVE   = Path("mock_s3/archive")
MANIFEST_FILE    = Path("mock_s3/manifest.json")

for bucket in [BUCKET_LANDING, BUCKET_PROCESSED, BUCKET_ARCHIVE]:
    bucket.mkdir(parents=True, exist_ok=True)

# Utility 
def file_checksum(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        h.update(f.read())
    return h.hexdigest()


def load_manifest() -> dict:
    if MANIFEST_FILE.exists():
        return json.loads(MANIFEST_FILE.read_text())
    return {"processed_files": [], "run_log": []}


def save_manifest(m: dict):
    MANIFEST_FILE.write_text(json.dumps(m, indent=2, default=str))

# generate raw CSVs into Landing bucket 
def ingest_raw_data(n_files=4, rows_per_file=5000):
    """
    Simulates upstream systems dropping CSV files into S3 Landing.
    Each file represents one day of transactions from a different source.
    """
    log.info("INGEST — Writing raw files to landing bucket...")
    sources = ["web_store", "mobile_app", "pos_terminal", "partner_api"]
    categories = ["Electronics", "Clothing", "Grocery", "Sports", "Home"]

    files_written = []
    for i, source in enumerate(sources[:n_files]):
        rows = []
        base_date = datetime(2024, 3, 1) + timedelta(days=i)
        for _ in range(rows_per_file):
            qty   = np.random.randint(1, 8)
            price = round(np.random.uniform(5, 500), 2)
            rows.append({
                "transaction_id": f"TXN{np.random.randint(1_000_000, 9_999_999)}",
                "timestamp":      base_date + timedelta(
                                    hours=np.random.randint(0, 23),
                                    minutes=np.random.randint(0, 59)),
                "customer_id":    f"C{np.random.randint(1000, 9999)}",
                "product_id":     f"P{np.random.randint(100, 999)}",
                "category":       np.random.choice(categories),
                "source":         source,
                "quantity":       qty,
                "unit_price":     price,
                "total":          round(qty * price, 2),
                "discount_pct":   np.random.choice([0, 5, 10, 15, 20]),
                "country":        np.random.choice(["US", "CA", "UK", "AU", "DE"]),
                "status":         np.random.choice(
                    ["completed", "completed", "completed", "refunded", "pending"]),
            })

        filename = f"transactions_{source}_{base_date.strftime('%Y%m%d')}.csv"
        path = BUCKET_LANDING / filename
        pd.DataFrame(rows).to_csv(path, index=False)
        files_written.append(filename)
        log.info(f"  Landed: {filename}  ({rows_per_file:,} rows)")
    return files_written 

 # Step 2: Transform — Lambda-style function
def lambda_transform_handler(event: dict) -> dict:
    """
    Mimics an AWS Lambda handler triggered by S3 PutObject events.
    Applies transformations and loads to the Processed bucket.
    """
    filename = event["Records"][0]["s3"]["object"]["key"]
    source_path = BUCKET_LANDING / filename
    log.info(f"  Lambda triggered for: {filename}")

    df = pd.read_csv(source_path, parse_dates=["timestamp"])
    original_rows = len(df)

    # Transformations
    # 1. Deduplicate on transaction_id
    df = df.drop_duplicates(subset=["transaction_id"])

    # 2. Drop cancelled / pending transactions
    df = df[df["status"] == "completed"]

    # 3. Compute net revenue after discount
    df["net_revenue"] = (df["total"] * (1 - df["discount_pct"] / 100)).round(2)

    # 4. Parse date parts
    df["date"]    = df["timestamp"].dt.date
    df["hour"]    = df["timestamp"].dt.hour
    df["weekday"] = df["timestamp"].dt.day_name()

    # 5. Tag high-value transactions
    threshold = df["net_revenue"].quantile(0.90)
    df["is_high_value"] = df["net_revenue"] >= threshold

    # 6. Standardise country codes
    df["country"] = df["country"].str.upper().str.strip()

    # 7. Remove outliers: net_revenue < 0 or > 99th percentile
    p99 = df["net_revenue"].quantile(0.99)
    df = df[(df["net_revenue"] > 0) & (df["net_revenue"] <= p99)]

    transformed_rows = len(df)
    drop_pct = (1 - transformed_rows / original_rows) * 100

    log.info(f"    Rows: {original_rows:,} → {transformed_rows:,}  "
             f"(dropped {drop_pct:.1f}%)")

    # Write to Processed bucket 
    out_name = filename.replace(".csv", "_transformed.parquet")
    out_path = BUCKET_PROCESSED / out_name
    df.to_parquet(out_path, index=False)
    log.info(f"    Saved: {out_name}")

    # Archive raw file 
    shutil.move(str(source_path), str(BUCKET_ARCHIVE / filename))

    return {
        "statusCode": 200,
        "source_file": filename,
        "output_file": out_name,
        "original_rows": original_rows,
        "transformed_rows": transformed_rows,
        "checksum": file_checksum(out_path),
    }
# Load / Aggregate
def load_and_aggregate():
    """Reads all transformed parquet files and builds final summary tables."""
    parquet_files = list(BUCKET_PROCESSED.glob("*.parquet"))
    if not parquet_files:
        log.warning("No processed files found.")
        return None

    log.info(f"LOAD — Reading {len(parquet_files)} parquet files...")
    df = pd.concat([pd.read_parquet(f) for f in parquet_files], ignore_index=True)
    log.info(f"  Combined dataset: {len(df):,} rows")

    os.makedirs("output", exist_ok=True)

    # Daily revenue summary
    daily = df.groupby("date").agg(
        transactions=("transaction_id", "count"),
        revenue=("net_revenue", "sum"),
        avg_order=("net_revenue", "mean"),
        unique_customers=("customer_id", "nunique"),
    ).reset_index()
    daily.to_csv("output/daily_summary.csv", index=False)

    # Category performance
    cat = df.groupby("category").agg(
        revenue=("net_revenue", "sum"),
        transactions=("transaction_id", "count"),
        avg_discount=("discount_pct", "mean"),
    ).reset_index().sort_values("revenue", ascending=False)
    cat.to_csv("output/category_summary.csv", index=False)

    # Source / channel breakdown
    source = df.groupby("source").agg(
        revenue=("net_revenue", "sum"),
        transactions=("transaction_id", "count"),
    ).reset_index()
    source.to_csv("output/source_summary.csv", index=False)

    # Country summary
    country = df.groupby("country")["net_revenue"].sum().reset_index(name="revenue")
    country.to_csv("output/country_summary.csv", index=False)

    # Final combined
    df.to_parquet("output/final_dataset.parquet", index=False)

    log.info("  Saved all summary CSVs and final parquet to output/")
    return df, daily, cat
# pipeline runner 
def run_pipeline():
    log.info("=" * 60)
    log.info("  AWS ETL PIPELINE — LOCAL SIMULATION")
    log.info("=" * 60)

    manifest = load_manifest()
    t_start  = time.time()

    # 1. Ingest
    landed_files = ingest_raw_data(n_files=4, rows_per_file=5000)

    # 2. Transform (Lambda invocations)
    log.info("\nTRANSFORM — Invoking Lambda handlers...")
    run_results = []
    for filename in landed_files:
        if filename in manifest["processed_files"]:
            log.info(f"  Skipping (already processed): {filename}")
            continue
        event = {"Records": [{"s3": {"object": {"key": filename}}}]}
        result = lambda_transform_handler(event)
        run_results.append(result)
        manifest["processed_files"].append(filename)

    # 3. Load & aggregate
    log.info("\nAGGREGATE — Building summary tables...")
    output = load_and_aggregate()

    # 4. Log run
    elapsed = round(time.time() - t_start, 2)
    run_entry = {
        "run_time":        datetime.now().isoformat(),
        "files_processed": len(run_results),
        "elapsed_seconds": elapsed,
        "status":          "SUCCESS",
    }
    manifest["run_log"].append(run_entry)
    save_manifest(manifest)

    # 5. Summary
    log.info("\n" + "=" * 60)
    log.info("  PIPELINE COMPLETE")
    log.info(f"  Files processed  : {len(run_results)}")
    log.info(f"  Manifest updated : {MANIFEST_FILE}")
    log.info("=" * 60)

    if output:
        df, daily, cat = output
        log.info(f"\n  Total Net Revenue : ${df['net_revenue'].sum():>12,.2f}")
        log.info(f"  Total Transactions: {len(df):>12,}")
        log.info(f"\n  Top Category      : {cat.iloc[0]['category']}  "
                 f"(${cat.iloc[0]['revenue']:,.2f})")


if __name__ == "__main__":
    # Clean up any prior run for a fresh demo
    for bucket in [BUCKET_LANDING, BUCKET_PROCESSED, BUCKET_ARCHIVE]:
        shutil.rmtree(bucket, ignore_errors=True)
        bucket.mkdir(parents=True, exist_ok=True)
    if Path("output").exists():
        shutil.rmtree("output")

    run_pipeline()