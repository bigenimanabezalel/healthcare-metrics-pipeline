"""
glue_etl_job.py

AWS Glue ETL job -- Stage 4 of the pipeline (cleaning / transformation).

This is written as a Glue PYTHON SHELL job (pandas-based), not a full
PySpark Glue job. Given the actual data volume here -- one quarter of PBJ
data (~1.3M rows) plus two facility-level files (~15,000 rows each) --
Spark's distributed-processing overhead buys nothing; pandas handles this
comfortably and Python Shell jobs are cheaper and start faster than a
Spark job with a warm-up cluster. If this pipeline later ingests multiple
years of PBJ data across many quarters, revisit this choice -- that's the
point where a PySpark Glue job would start to pay for itself.

What this job does, per file:
  1. Reads the raw CSV from S3, forcing the CCN/PROVNUM column to string
     type on read -- this is the fix for the leading-zero bug the NH Data
     Dictionary explicitly warns about (Excel strips leading zeros from
     CCN codes; reading as string prevents that here too).
  2. Normalizes EVERY column name generically -- handles both punctuation
     boundaries ("Number of Certified Beds") and camelCase boundaries with
     no space at all ("WorkDate") -- so "WorkDate" and "Work Date" both
     converge to "workdate" regardless of which style the real file
     actually uses, instead of silently failing to rename and crashing on
     the next line. (One known exception: "MDScensus" is genuinely
     ambiguous -- there's no case-transition cue to split "MDS" from
     "census" -- so it's handled with an explicit alias instead, below.)
     A small number of columns then get an additional, deliberately
     SHORTER rename on top of that (e.g. the normalized
     "cms_certification_number_ccn" -> "ccn") where the generic
     normalization alone would produce something needlessly long.
  3. Parses date columns properly.
  4. Deduplicates on each file's natural key.
  5. Writes out as Parquet to S3 processed -- overwriting the existing
     file each run. Since CMS publishes whole-file snapshots (not
     row-level deltas), a full overwrite of just the affected file is the
     correct way to handle a corrected/reissued file: it replaces stale
     rows rather than appending duplicates alongside them.

Deployment notes
-----------------
- Job type: Python Shell (not Spark)
- Python library set: pandas, pyarrow, boto3, s3fs (available via Glue's
  standard Python Shell library list, or bundled as a wheel if not)
- Trigger: an S3 event notification (via EventBridge) on new objects
  landing under the raw/ prefix, OR a simple schedule shortly after the
  Lambda extractor's schedule -- either works given the small file count.
- IAM permissions needed: s3:GetObject on the raw bucket,
  s3:PutObject on the processed bucket.
"""

import io
import re

import boto3
import pandas as pd

S3_BUCKET = "de-healthcare-project-bucket"
RAW_PREFIX = "raw/"
PROCESSED_PREFIX = "processed/"

s3 = boto3.client("s3")


def normalize_column_name(name: str) -> str:
    """Generic snake_case normalization, applied BEFORE the semantic
    renames below. Handles two different kinds of word boundaries so that
    stylistic variants of the same column name converge to one result:
      - punctuation/spacing boundaries: "Number of Certified Beds"
      - camelCase boundaries (no space at all): "WorkDate"
    Without the camelCase split, "WorkDate" and "Work Date" would
    normalize to two DIFFERENT names ("workdate" vs "work_date") --
    tested and confirmed as a real gap in an earlier version of this
    function, so this handles both consistently now."""
    name = name.strip()
    name = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name)  # split camelCase: WorkDate -> Work_Date
    name = name.lower()
    name = re.sub(r"[^0-9a-z]+", "_", name)  # spaces, parentheses, etc. -> underscore
    name = re.sub(r"_+", "_", name).strip("_")  # collapse repeats, trim edges
    return name


def read_csv_from_s3(key: str, dtype_original_names: dict) -> pd.DataFrame:
    """dtype_original_names uses the ORIGINAL (pre-normalization) column
    name as the key, since dtype must be specified before we've read the
    file at all. Column names are normalized immediately after reading."""
    obj = s3.get_object(Bucket=S3_BUCKET, Key=f"{RAW_PREFIX}{key}")
    # encoding="cp1252": CMS CSV exports (NH_ProviderInfo, QRP data) commonly
    # use Windows-1252, not UTF-8 -- provider names/notes with an apostrophe
    # often save as the cp1252 "smart quote" byte (0x92), which UTF-8 (pandas'
    # default) rejects outright with a UnicodeDecodeError. cp1252 can decode
    # every byte 0-255, so this never fails on messy government export text.
    df = pd.read_csv(
        io.BytesIO(obj["Body"].read()),
        dtype=dtype_original_names,
        low_memory=False,
        encoding="cp1252",
    )
    df.columns = [normalize_column_name(c) for c in df.columns]

    # Explicit aliases for cases where the generic normalizer's technically-
    # correct output doesn't match this project's established naming (both
    # confirmed against the real PBJ file -- see conversation history):
    #   - "mds_census" -> "mdscensus": MDScensus has no case-transition cue
    #     to split on, so it's handled as an exception either way.
    #   - "work_date" -> "workdate": WorkDate WOULD split correctly via the
    #     camelCase rule above, but the real file already confirmed this
    #     column as "WorkDate" -- so the extra underscore isn't needed for
    #     correctness, just reverted here to keep the simpler, already-
    #     established "workdate" name used everywhere else in the project.
    #     The camelCase-splitting rule itself stays in normalize_column_name()
    #     as a safety net for the two files (NH_ProviderInfo, QRP) not yet
    #     verified against real headers.
    df = df.rename(columns={"mds_census": "mdscensus", "work_date": "workdate"})

    return df


def write_parquet_to_s3(df: pd.DataFrame, key: str) -> None:
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.upload_fileobj(buffer, S3_BUCKET, f"{PROCESSED_PREFIX}{key}")
    print(f"Wrote {len(df):,} rows -> s3://{S3_BUCKET}/{PROCESSED_PREFIX}{key}")


# ---------------------------------------------------------------------------
# 1. PBJ Daily Nurse Staffing (master file)
# ---------------------------------------------------------------------------
def clean_pbj(raw_filename: str) -> pd.DataFrame:
    # PBJ's original headers (PROVNUM, WorkDate, Hrs_RN_emp, ...) already
    # use underscores between words, so generic normalization alone
    # produces exactly the target names -- no semantic rename dict needed
    # here, unlike the two CMS catalog files below.
    # WorkDate forced to str on read too: the real file stores it as a plain
    # 8-digit number like 20240401 (YYYYMMDD), and reading it as int first
    # then parsing risks pandas treating it as nanosecond-epoch input (see
    # the format= fix below) rather than a date -- forcing str here keeps
    # the value exactly as written until we explicitly parse it.
    df = read_csv_from_s3(raw_filename, dtype_original_names={"PROVNUM": str, "WorkDate": str})

    keep_cols = [
        "provnum", "workdate", "mdscensus",
        "hrs_rn", "hrs_rn_emp", "hrs_rn_ctr",
        "hrs_lpn", "hrs_lpn_emp", "hrs_lpn_ctr",
        "hrs_cna", "hrs_cna_emp", "hrs_cna_ctr",
    ]
    df = df[keep_cols]

    # format="%Y%m%d" is required here: WorkDate is stored as a plain
    # 8-digit number (20240401), not a recognizable date string. Without an
    # explicit format, pd.to_datetime() on a bare integer-like value treats
    # it as nanoseconds since 1970-01-01 instead of a YYYYMMDD date -- that
    # produces a timestamp with leftover nanosecond precision that Parquet
    # (microsecond precision) can't store, causing
    # "ArrowInvalid: Casting from timestamp[ns] to timestamp[us] would lose
    # data" at write time. Parsing with the correct format avoids that
    # entirely since a calendar date has no sub-day component to lose.
    df["workdate"] = pd.to_datetime(df["workdate"], format="%Y%m%d", errors="coerce")
    numeric_cols = [c for c in keep_cols if c not in ("provnum", "workdate")]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    before = len(df)
    df = df.drop_duplicates(subset=["provnum", "workdate"])
    print(f"PBJ: dropped {before - len(df):,} duplicate rows (key: provnum + workdate)")

    return df


# ---------------------------------------------------------------------------
# 2. NH Provider Information
# ---------------------------------------------------------------------------
def clean_provider_info(raw_filename: str) -> pd.DataFrame:
    df = read_csv_from_s3(raw_filename, dtype_original_names={"CMS Certification Number (CCN)": str})

    # After generic normalization, "Provider Name" -> "provider_name",
    # "State" -> "state", "Provider Type" -> "provider_type", and
    # "Total nursing staff turnover" -> "total_nursing_staff_turnover"
    # already match the target names -- only these 3 need an explicit,
    # deliberately SHORTER name than what normalization alone would give:
    df = df.rename(columns={
        "cms_certification_number_ccn": "ccn",
        "number_of_certified_beds": "certified_beds",
        "registered_nurse_turnover": "rn_turnover",
    })

    keep_cols = [
        "ccn", "provider_name", "state", "provider_type",
        "certified_beds", "total_nursing_staff_turnover", "rn_turnover",
    ]
    df = df[keep_cols]

    for col in ["certified_beds", "total_nursing_staff_turnover", "rn_turnover"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    before = len(df)
    df = df.drop_duplicates(subset=["ccn"])
    print(f"NH_ProviderInfo: dropped {before - len(df):,} duplicate rows (key: ccn)")

    return df


# ---------------------------------------------------------------------------
# 3. SNF QRP Provider Data (long format -- one row per CCN per measure)
# ---------------------------------------------------------------------------
def clean_qrp_provider_data(raw_filename: str) -> pd.DataFrame:
    df = read_csv_from_s3(raw_filename, dtype_original_names={"CMS Certification Number (CCN)": str})

    # "Measure Code" -> "measure_code", "Score" -> "score",
    # "Start Date" -> "start_date", "End Date" -> "end_date" already match
    # after generic normalization -- only CCN needs shortening.
    df = df.rename(columns={"cms_certification_number_ccn": "ccn"})

    keep_cols = ["ccn", "measure_code", "score", "start_date", "end_date"]
    df = df[keep_cols]

    df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce")
    df["end_date"] = pd.to_datetime(df["end_date"], errors="coerce")

    before = len(df)
    df = df.drop_duplicates(subset=["ccn", "measure_code", "start_date", "end_date"])
    print(f"QRP provider data: dropped {before - len(df):,} duplicate rows "
          f"(key: ccn + measure_code + start_date + end_date)")

    return df


def main():
    # NOTE: match these raw filenames exactly to what Lambda actually wrote
    # to S3 raw -- adjust if your real filenames differ slightly.
    pbj_df = clean_pbj("PBJ_Daily_Nurse_Staffing_Q2_2024.csv")
    write_parquet_to_s3(pbj_df, "pbj_daily_nurse_staffing.parquet")

    provider_df = clean_provider_info("NH_ProviderInfo_Oct2024.csv")
    write_parquet_to_s3(provider_df, "nh_providerinfo.parquet")

    qrp_df = clean_qrp_provider_data(
        "Skilled_Nursing_Facility_Quality_Reporting_Program_Provider_Data_Oct2024.csv"
    )
    write_parquet_to_s3(qrp_df, "qrp_provider_data.parquet")

    print("Glue ETL job complete.")


if __name__ == "__main__":
    main()
