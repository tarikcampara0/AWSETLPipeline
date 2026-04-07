
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
