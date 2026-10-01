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

This keeps the whole pipeline inside one service family (Glue) instead of
Lambda + EventBridge + Glue for what is fundamentally one sequential batch
job. The per-invocation cost difference between Lambda and Glue Python
Shell is real but small at this data volume -- outweighed here by having
one service to monitor, one console to check run history in, and one
Workflow that owns the entire schedule and sequencing.

The master PBJ file and the supporting files live in two SEPARATE Drive
locations (confirmed from the project brief's own links -- the master file
is shared as a single standalone file, not inside the supporting-files
folder), so this job fetches them two different ways:
  - The master file: fetched directly by its own file ID (MASTER_FILE_ID).
  - The supporting files: listed from a shared folder (DRIVE_FOLDER_ID)
    and filtered down to the ones this project needs.

What this job does, per run:
  1. Looks up the master file directly by ID, and lists files in the
     configured supporting-files folder, filtered to only the files this
     project actually needs (see WATCHED_FILE_PATTERNS).
  2. For each of those files (master + watched supporting files), compares
     its Drive `modifiedTime` against the watermark stored in DynamoDB
     from the last run.
  3. Downloads and uploads to S3 raw (Bronze) ONLY if new or changed.
     Unchanged files are skipped -- this is what makes the sync
     incremental, same mechanism as the retired Lambda version.
  4. Updates the DynamoDB watermark for each file it downloaded.

Deployment notes
-----------------
- Job type: Python Shell (not Spark) -- this is a lightweight API-polling
  task, not a data-processing task; Spark's distributed overhead buys
  nothing here.
- Python library set: google-api-python-client, google-auth, boto3
  (available via Glue's Python Shell library list, or bundled as a wheel).
- Orchestration: a node in the Glue Workflow for this pipeline, with the
  Workflow's own ON_SCHEDULE trigger starting this job -- no separate
  EventBridge rule needed. On successful completion, the Workflow's next
  trigger fires the transformation job (glue_etl_job.py).
- IAM permissions needed: s3:PutObject on the raw bucket,
  dynamodb:GetItem/PutItem on the watermark table,
  secretsmanager:GetSecretValue on the Drive credentials secret.
- Configuration (Glue job parameters, passed as --key value at job
  creation/run time, read via getResolvedOptions below):
    --MASTER_FILE_ID, --DRIVE_FOLDER_ID, --S3_BUCKET, --DYNAMODB_TABLE,
    --DRIVE_CREDENTIALS_SECRET_ARN, --S3_RAW_PREFIX (optional)
- The Google service account must have "Viewer" access shared to it on
  BOTH the master file and the supporting-files folder individually --
  sharing is per-item in Drive, so granting access to the folder does NOT
  automatically grant access to the separately-shared master file. A
  personal Google login cannot be used from a headless AWS job either
  way, same requirement as the retired Lambda version.
"""

import json
import sys
from io import BytesIO

import boto3
from awsglue.utils import getResolvedOptions
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# --- Config from Glue job parameters ---
args = getResolvedOptions(sys.argv, [
    "MASTER_FILE_ID", "DRIVE_FOLDER_ID", "S3_BUCKET", "DYNAMODB_TABLE",
    "DRIVE_CREDENTIALS_SECRET_ARN",
])
MASTER_FILE_ID = args["MASTER_FILE_ID"]  # the PBJ master file's own Drive file ID
DRIVE_FOLDER_ID = args["DRIVE_FOLDER_ID"]  # the supporting-files folder's ID
S3_BUCKET = args["S3_BUCKET"]
DYNAMODB_TABLE = args["DYNAMODB_TABLE"]
DRIVE_CREDENTIALS_SECRET_ARN = args["DRIVE_CREDENTIALS_SECRET_ARN"]
S3_RAW_PREFIX = "raw/"

# Only these 2 supporting files feed the project's selected metrics (see
# metrics_feasibility_audit.md) -- restricting scope here means storage
# and processing costs stay proportional to what's actually used, not the
# full 18-file supporting-file catalog. The master PBJ file isn't listed
# here since it's fetched directly by MASTER_FILE_ID, not filtered out of
# a folder listing.
WATCHED_FILE_PATTERNS = [
    "nh_providerinfo",
    "skilled_nursing_facility_quality_reporting_program_provider_data",
]

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
secrets_manager = boto3.client("secretsmanager")


def get_drive_service():
    """Authenticate to the Google Drive API using a service account key
    stored in Secrets Manager (never hardcoded or committed to source)."""
    secret_value = secrets_manager.get_secret_value(SecretId=DRIVE_CREDENTIALS_SECRET_ARN)
    credentials_info = json.loads(secret_value["SecretString"])
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info,
        scopes=["https://www.googleapis.com/auth/drive.readonly"],
    )
    return build("drive", "v3", credentials=credentials)


def get_file_metadata(drive_service, file_id: str) -> dict:
    """Look up a single file directly by its Drive file ID -- used for the
    master PBJ file, which is shared as a standalone file link rather than
    living inside the supporting-files folder, so it can't be found by
    listing a folder at all."""
    return drive_service.files().get(
        fileId=file_id, fields="id, name, modifiedTime",
    ).execute()


def list_watched_files(drive_service) -> list:
    """List every file in the configured folder, filtered down to only
    the ones this project needs (see WATCHED_FILE_PATTERNS)."""
    results = []
    page_token = None
    while True:
        response = drive_service.files().list(
            q=f"'{DRIVE_FOLDER_ID}' in parents and trashed = false",
            fields="nextPageToken, files(id, name, modifiedTime)",
            pageToken=page_token,
        ).execute()

        for f in response.get("files", []):
            name_lower = f["name"].lower()
            if any(pattern in name_lower for pattern in WATCHED_FILE_PATTERNS):
                results.append(f)

        page_token = response.get("nextPageToken")
        if not page_token:
            break

    return results


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


def download_and_upload(drive_service, file_id: str, file_name: str) -> None:
    """Stream a file's bytes from Google Drive straight into S3, without
    writing to local disk."""
    request = drive_service.files().get_media(fileId=file_id)
    buffer = BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)

    done = False
    while not done:
        _, done = downloader.next_chunk()

    buffer.seek(0)
    s3_key = f"{S3_RAW_PREFIX}{file_name}"
    s3.upload_fileobj(buffer, S3_BUCKET, s3_key)
    print(f"Uploaded {file_name} -> s3://{S3_BUCKET}/{s3_key}")


def main():
    drive_service = get_drive_service()

    # The master file (direct lookup by ID) and the supporting files
    # (folder listing, filtered) are fetched two different ways, then
    # processed identically from here on -- the incremental watermark
    # check and download logic doesn't care which source a file came from.
    master_file = get_file_metadata(drive_service, MASTER_FILE_ID)
    supporting_files = list_watched_files(drive_service)
    all_files = [master_file] + supporting_files

    downloaded = []
    skipped = []

    for f in all_files:
        file_id, file_name, modified_time = f["id"], f["name"], f["modifiedTime"]
        last_seen = get_watermark(file_id)

        if last_seen == modified_time:
            skipped.append(file_name)
            continue  # unchanged since last run -- this is the incremental check

        download_and_upload(drive_service, file_id, file_name)
        set_watermark(file_id, file_name, modified_time)
        downloaded.append(file_name)

    result = {"downloaded": downloaded, "skipped": skipped}
    print(json.dumps(result))

    # If nothing new arrived, the downstream transformation job (next node
    # in the Glue Workflow) still runs -- it's cheap and idempotent since
    # it always reads whatever's currently in S3 raw. An alternative some
    # teams prefer is to have this job raise/exit non-zero when
    # `downloaded` is empty, so the Workflow's on-success trigger for the
    # transformation job doesn't fire at all -- worth deciding based on
    # how much you want to avoid even a no-op transformation run.
    return result


if __name__ == "__main__":
    main()
