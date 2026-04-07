
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
