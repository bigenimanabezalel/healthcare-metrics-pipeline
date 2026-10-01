# Healthcare Metrics Pipeline

An end-to-end data engineering pipeline for nursing home staffing and quality metrics, built entirely on AWS, sourced from CMS (Centers for Medicare & Medicaid Services) public data.

**Live dashboard:** https://healthcare-metrics-pipeline-akmqhzfqesllmetnrcf66j.streamlit.app/

## What this project does

The pipeline ingests CMS nursing home data (PBJ daily staffing, Provider Info, and SNF Quality Reporting Program data) from a shared Google Drive source, cleans and transforms it, and serves it through an interactive dashboard tracking:

- Total nursing hours worked, by state and month
- Occupancy rate trends over time
- Staffing levels vs. occupancy rate
- Staffing levels vs. readmission rate
- Contract vs. employed staffing ratio
- Average staffing levels by state and facility type
- Nursing staff turnover, by state

## Architecture

```
Google Drive (source CSVs)
        |
        v
AWS Glue (Python Shell) — incremental ingestion
   watermarked via DynamoDB, polls Drive REST API directly
        |
        v
S3 (raw/) — bronze zone, original CSVs
        |
        v
AWS Glue (Python Shell) — ETL / transformation
   column normalization, type fixes, deduplication
        |
        v
S3 (processed/) — Parquet, overwritten per run
        |
        v
Streamlit app — queries S3 directly via DuckDB's httpfs
   (no warehouse, no Athena/Glue Catalog needed)
```

Both Glue jobs are orchestrated by a single **Glue Workflow**: a schedule trigger fires the ingestion job, and an on-success trigger chains into the ETL job once ingestion completes.

## Why these design choices

- **Glue Python Shell, not PySpark** — the data volume (~1.3M PBJ rows per quarter, ~15K rows per catalog file) doesn't need Spark's distributed processing. Pandas handles it comfortably, and Python Shell jobs start faster and cost less than a warm Spark cluster.
- **Glue Workflow, not Lambda + EventBridge** — ingestion is a periodic batch check, not an event-driven workload, so Glue's own workflow orchestration covers both the schedule and the job-chaining without adding extra AWS services.
- **Plain `urllib` for Google Drive, not `google-api-python-client`** — only two REST calls are needed (file metadata, file bytes), both doable with HTTPS GET requests. This avoids a ~27MB dependency tree with compiled C extensions that risk breaking against Glue's Python version.
- **DuckDB over a data warehouse** — `read_parquet()` can query S3 directly with no Athena/Glue Catalog setup, keeping the dashboard's query layer serverless and dependency-free.

## Repo structure

| File | Purpose |
|---|---|
| `streamlit_app.py` | The dashboard — queries processed Parquet in S3 via DuckDB |
| `glue_ingestion_job.py` | Glue job: incremental sync from Google Drive to S3 raw |
| `glue_etl_job.py` | Glue job: cleans, normalizes, and writes Parquet to S3 processed |
| `healthcare_eda.py` | Standalone EDA script — missing values, duplicates, outlier profiling |
| `metrics_feasibility_audit.md` | Metric-by-metric reasoning on what's calculable from the available CMS files, and what isn't |
| `requirements.txt` | Python dependencies for the Streamlit app |

## Data limitations

Not every requested metric is calculable from the available CMS files. Notably:

- **Overtime hours** can't be isolated — PBJ reports total hours per facility per day, not per employee or shift.
- **Length of stay** can't be calculated at all — no file records admission/discharge dates.
- **Cost metrics** (payroll, cost per stay, revenue) aren't calculable — no wage, billing, or revenue data exists in any source file.
- **Department-level breakdowns** aren't possible — PBJ has no unit/ward field.

See `metrics_feasibility_audit.md` for the full reasoning across all requested metrics.

## Running locally

```bash
pip install -r requirements.txt
streamlit run streamlit_app.py
```

Requires a `.streamlit/secrets.toml` with read-only AWS credentials:

```toml
[aws]
aws_access_key_id = "..."
aws_secret_access_key = "..."
region = "us-east-1"
```

The IAM user behind these credentials is scoped to `s3:GetObject` and `s3:ListBucket` on a single bucket only — no write access, no access to any other AWS resource.
