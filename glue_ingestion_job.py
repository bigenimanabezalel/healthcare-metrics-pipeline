"""
glue_ingestion_job.py

AWS Glue Python Shell job -- Stage 2 of the pipeline (incremental
ingestion from Google Drive). REPLACES the earlier Lambda-based
extractor, per review feedback: this is a periodic BATCH check, not a
real-time event-driven workload, so there's no need for Lambda's
event-driven execution model at all -- Glue Python Shell does the same
job, and a single Glue Workflow can orchestrate both this ingestion step
and the downstream transformation step (glue_etl_job.py) without adding
EventBridge or Step Functions as separate services on top.

AUTHENTICATION -- a plain API key, no client library needed at all
-----------------------------------------------------------------
All 3 files this project needs were confirmed shared with "Anyone with the
link" access, and each one's own individual file ID is known up front:
    - PBJ master file
    - NH_ProviderInfo
    - SNF QRP Provider Data

Earlier versions of this job used Google's `google-api-python-client`
library to talk to the Drive API. That library is built to support EVERY
Google API (Sheets, Calendar, YouTube, OAuth flows, service accounts...)
-- we only need two plain operations ("get this file's metadata", "get
this file's bytes"), so pulling in the whole client library dragged in a
~27MB dependency tree, including compiled C extensions (cryptography,
cffi) that risk breaking if their build doesn't match Glue's Python
version exactly.

Both operations we actually need are just HTTPS GET requests to Google's
REST API:
    https://www.googleapis.com/drive/v3/files/{file_id}?fields=...&key=...
    https://www.googleapis.com/drive/v3/files/{file_id}?alt=media&key=...

Python's built-in `urllib.request` can make both calls directly -- no
external package, no library zip to build, no Python-version
compatibility risk. This is the whole reason Step 5 ("package a library
zip for Glue") disappears entirely with this version of the script.

What this job does, per run:
  1. For each of the 3 known file IDs, calls the Drive REST API directly
     to get its current `modifiedTime` (no folder listing involved).
  2. Compares that against the watermark stored in DynamoDB from the last
     run.
  3. Downloads and uploads to S3 raw (Bronze) ONLY if new or changed.
     Unchanged files are skipped -- this is what makes the sync
     incremental.
  4. Updates the DynamoDB watermark for each file it downloaded.

Deployment notes
-----------------
- Job type: Python Shell (not Spark) -- this is a lightweight API-polling
  task, not a data-processing task.
- Python library set: NONE needed beyond what Glue Python Shell already
  ships with (boto3, and the standard library's urllib/json/io). No
  "Python library path" setting needs to be configured for this job.
- Orchestration: a node in the Glue Workflow for this pipeline, with the
  Workflow's own schedule trigger starting this job. On successful
  completion, the Workflow's on-success trigger fires the transformation
  job (glue_etl_job.py).
- IAM permissions needed: s3:PutObject on the raw bucket,
  dynamodb:GetItem/PutItem on the watermark table.
- Configuration (Glue job parameters, passed as --key value at job
  creation/run time, read via getResolvedOptions below):
    --DRIVE_API_KEY, --S3_BUCKET, --DYNAMODB_TABLE
  DRIVE_API_KEY comes from Google Cloud Console -> APIs & Services ->
  Credentials -> Create API key, restricted to the "Google Drive API"
  only.
- No Drive-side sharing step needed: the files are already shared "Anyone
  with the link can view", which is exactly the access level the Drive
  REST API relies on when called with just an API key.
"""

import json
import sys
import urllib.parse
import urllib.request

import boto3
from awsglue.utils import getResolvedOptions

# --- Config from Glue job parameters ---
args = getResolvedOptions(sys.argv, ["DRIVE_API_KEY", "S3_BUCKET", "DYNAMODB_TABLE"])
DRIVE_API_KEY = args["DRIVE_API_KEY"]
S3_BUCKET = args["S3_BUCKET"]
DYNAMODB_TABLE = args["DYNAMODB_TABLE"]
S3_RAW_PREFIX = "raw/"

DRIVE_API_BASE = "https://www.googleapis.com/drive/v3/files"

# The 3 files this project needs, by their own Drive file ID -- confirmed
# directly from the individual "Get link" shares (not discovered by
# listing a folder). See metrics_feasibility_audit.md for why only these
# 3 files feed the project's selected metrics.
WATCHED_FILE_IDS = [
    "1kZMZFGfTLdcwmdhjDPZh2-XE2_gOBRCz",  # PBJ_Daily_Nurse_Staffing_Q2_2024.csv (master)
    "1gsofjXa-DHRPgPw0iZQa3cl4VqVP74jb",  # NH_ProviderInfo_Oct2024.csv
    "1qITfNH6YwL2LfIatnjMHZQMW2OX9dwG6",  # Skilled_Nursing_Facility_..._Provider_Data_Oct2024.csv
]

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")


def get_file_metadata(file_id: str) -> dict:
    """GET a file's id/name/modifiedTime directly from the Drive REST API
    -- a plain HTTPS call, no client library needed. Using Drive's own
    returned `name` field (rather than a name we guess ourselves)
    guarantees the S3 key matches exactly what Drive calls the file,
    which is what glue_etl_job.py's filenames expect."""
    params = urllib.parse.urlencode({"fields": "id,name,modifiedTime", "key": DRIVE_API_KEY})
    url = f"{DRIVE_API_BASE}/{file_id}?{params}"
    with urllib.request.urlopen(url) as response:
        return json.loads(response.read())


def get_watermark(file_id: str):
    """Return the last-recorded modifiedTime for this file, or None if
    we've never seen it before."""
    table = dynamodb.Table(DYNAMODB_TABLE)
    response = table.get_item(Key={"file_id": file_id})
    item = response.get("Item")
    return item["modified_time"] if item else None


def set_watermark(file_id: str, file_name: str, modified_time: str) -> None:
    table = dynamodb.Table(DYNAMODB_TABLE)
    table.put_item(Item={
        "file_id": file_id,
        "file_name": file_name,
        "modified_time": modified_time,
    })


def download_and_upload(file_id: str, file_name: str) -> None:
    """Stream a file's bytes from the Drive REST API straight into S3,
    without writing to local disk."""
    params = urllib.parse.urlencode({"alt": "media", "key": DRIVE_API_KEY})
    url = f"{DRIVE_API_BASE}/{file_id}?{params}"
    with urllib.request.urlopen(url) as response:
        s3_key = f"{S3_RAW_PREFIX}{file_name}"
        s3.upload_fileobj(response, S3_BUCKET, s3_key)
    print(f"Uploaded {file_name} -> s3://{S3_BUCKET}/{s3_key}")


def main():
    downloaded = []
    skipped = []

    for file_id in WATCHED_FILE_IDS:
        metadata = get_file_metadata(file_id)
        file_name, modified_time = metadata["name"], metadata["modifiedTime"]
        last_seen = get_watermark(file_id)

        if last_seen == modified_time:
            skipped.append(file_name)
            continue  # unchanged since last run -- this is the incremental check

        download_and_upload(file_id, file_name)
        set_watermark(file_id, file_name, modified_time)
        downloaded.append(file_name)

    result = {"downloaded": downloaded, "skipped": skipped}
    print(json.dumps(result))

    # If nothing new arrived, the downstream transformation job (next node
    # in the Glue Workflow) still runs -- it's cheap and idempotent since
    # it always reads whatever's currently in S3 raw.
    return result


if __name__ == "__main__":
    main()
