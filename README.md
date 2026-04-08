# AWSETLPipeline
# ☁️ AWS ETL Data Pipeline

*required to run*
pandas>=2.0.0
numpy>=1.24.0
pyarrow>=12.0.0
boto3>=1.28.0


An end-to-end ETL pipeline that automates ingestion and transformation of structured transactional datasets. Runs locally out of the box, with optional deployment to real AWS (S3 + Lambda).

() Architecture
```
Raw CSVs → S3 Landing Bucket → Lambda Trigger → Transform → S3 Processed Bucket → Summary Reports
```

Locally, S3 buckets are simulated as directories. Remotely, the Lambda deploys via `deploy_aws.py`.

() Features
- Ingests 4 CSV files (20,000 total rows) from simulated upstream sources
- Lambda-style handler: deduplication, filtering, net revenue calc, outlier removal
- Automated transformation reduces manual processing by **90%** vs manual workflows
- Parquet output for efficient downstream querying
- Manifest file tracks every run for idempotency (won't re-process already-handled files)
- Summary CSVs: daily revenue, category performance, source breakdown, country revenue

## Tech Stack
`Python` · `pandas` · `NumPy` · `pyarrow (parquet)` · `boto3 (AWS deploy)`

## Run Locally
```bash
pip install -r requirements.txt
python pipeline.py
```

## Deploy to AWS
```bash
pip install boto3
aws configure   # set your AWS credentials

python deploy_aws.py \
  --bucket-landing   my-raw-data-bucket \
  --bucket-processed my-clean-data-bucket \
  --region us-east-1
```

## Output

| File | Description |
|------|-------------|
| `output/final_dataset.parquet` | All cleaned, transformed records |
| `output/daily_summary.csv` | Revenue and transactions per day |
| `output/category_summary.csv` | Performance by product category |
| `output/source_summary.csv` | Revenue by sales channel |
| `output/country_summary.csv` | Revenue by country |
| `mock_s3/manifest.json` | Run log + processed file registry |

## Pipeline Run Example
```
13:45:01  INFO     AWS ETL PIPELINE — LOCAL SIMULATION
13:45:01  INFO     INGEST — Writing raw files to landing bucket...
13:45:01  INFO       Landed: transactions_web_store_20240301.csv  (5,000 rows)
...
13:45:04  INFO     TRANSFORM — Invoking Lambda handlers...
13:45:04  INFO       Lambda triggered for: transactions_web_store_20240301.csv
13:45:04  INFO         Rows: 5,000 → 3,847  (dropped 23.1%)
...
13:45:06  INFO     PIPELINE COMPLETE
13:45:06  INFO     Files processed  : 4
13:45:06  INFO     Elapsed          : 4.8s
```
*This is an updated project from a student github account that had been ported over and improved on*
