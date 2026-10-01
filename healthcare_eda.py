"""
healthcare_eda.py

Initial Data Analysis for the Healthcare Metrics project (Step 2 of the
project plan): missing values, duplicates, and outliers across the master
PBJ staffing file and the supporting NH data catalog CSVs.

Requirements:
    pip install pandas numpy matplotlib boto3

Usage (local folder):
    python healthcare_eda.py --input-dir ./data --output-dir ./eda_output

Usage (S3 raw zone, no local download needed):
    python healthcare_eda.py --input-dir s3://de-healthcare-project-bucket/raw --output-dir ./eda_output

--input-dir accepts either a local folder path or an s3://bucket/prefix
path. Either way, every *.csv file found is profiled -- no need to list
them by name. Reading from S3 requires AWS credentials to be available the
usual way (environment variables, ~/.aws/credentials, or an IAM role) --
boto3 picks these up automatically, nothing to configure in this script.
Only the CSVs are streamed from S3 into memory for profiling; the plots
and text reports this script produces are still written to a local
--output-dir, since those are new files this script creates, not source
data.
"""

import argparse
import os
from io import BytesIO
from urllib.parse import urlparse

import boto3
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# Natural keys for duplicate-checking, mapped from the NH Data Dictionary's
# documented "Variable Name (Column Header)" for each table -- updated to
# match your actual file list. Filename matching is case-insensitive
# substring, so "NH_ProviderInfo_Oct2024.csv" matches "nh_providerinfo".
#
# NOTE: CMS Provider Data Catalog downloads typically use these exact
# display headers (with spaces/parentheses), but if your actual CSV headers
# differ slightly, guess_key_columns() falls back gracefully to a full-row
# duplicate check for that file -- worth spot-checking df.columns for the
# first run of each file to confirm.
KNOWN_KEYS = {
    "pbj_daily_nurse_staffing": ["PROVNUM", "WorkDate"],
    "nh_providerinfo": ["CMS Certification Number (CCN)"],
    "nh_ownership": ["CMS Certification Number (CCN)", "Owner Name", "Role played by Owner or Manager in Facility"],
    "nh_penalties": ["CMS Certification Number (CCN)", "Penalty Date", "Penalty Type"],
    "nh_surveydates": ["CMS Certification Number (CCN)", "Survey Date", "Type of Survey"],
    "nh_surveysummary": ["CMS Certification Number (CCN)", "Inspection Cycle"],
    "nh_firesafetycitations": ["CMS Certification Number (CCN)", "Survey Date", "Deficiency Tag Number"],
    "nh_healthcitations": ["CMS Certification Number (CCN)", "Survey Date", "Deficiency Tag Number"],
    "nh_citationdescriptions": ["Deficiency Prefix", "Deficiency Tag Number"],
    "nh_qualitymsr_mds": ["CMS Certification Number (CCN)", "Measure Code"],
    "nh_qualitymsr_claims": ["CMS Certification Number (CCN)", "Measure Code"],
    "nh_stateusaverages": ["State or Nation"],
    "nh_hlthinspeccutpointsstate": ["State"],
    "nh_datacollectionintervals": ["Measure Code"],
    "nh_covidvaxprovider": ["CMS Certification Number (CCN)"],
    "nh_covidvaxaverages": ["State"],
    "skilled_nursing_facility_quality_reporting_program_national_data": ["CMS Certification Number (CCN)", "Measure Code", "Start Date", "End Date"],
    "skilled_nursing_facility_quality_reporting_program_provider_data": ["CMS Certification Number (CCN)", "Measure Code", "Start Date", "End Date"],
    "swing_bed_snf_data": ["CMS Certification Number (CCN)", "Measure Code", "Start Date", "End Date"],
    "fy_2024_snf_vbp_facility": ["CMS Certification Number (CCN)"],
    # FY_2024_SNF_VBP_Aggregate_Performance has no key -- it's a single
    # national-level summary row, so it's intentionally left out here and
    # falls back to a full-row duplicate check.
}


def guess_key_columns(filename: str, columns: list) -> list | None:
    """Match a filename against KNOWN_KEYS (case-insensitive substring),
    falling back to None (full-row duplicate check) if nothing matches."""
    fname_lower = filename.lower()
    for pattern, key_cols in KNOWN_KEYS.items():
        if pattern in fname_lower:
            present = [c for c in key_cols if c in columns]
            if present:
                return present
    return None


def profile_missing_values(df: pd.DataFrame, name: str, output_dir: str) -> pd.DataFrame:
    missing_count = df.isna().sum()
    missing_pct = (missing_count / len(df) * 100).round(2)
    report = pd.DataFrame({"missing_count": missing_count, "missing_pct": missing_pct})
    report = report[report["missing_count"] > 0].sort_values("missing_pct", ascending=False)

    if not report.empty:
        plt.figure(figsize=(10, max(4, len(report) * 0.3)))
        plt.barh(report.index.astype(str), report["missing_pct"])
        plt.xlabel("% missing")
        plt.title(f"Missing values by column -- {name}")
        plt.gca().invert_yaxis()
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, f"{name}_missing_values.png"))
        plt.close()

    return report


def profile_duplicates(df: pd.DataFrame, key_cols: list | None) -> dict:
    full_row_dupes = df.duplicated().sum()
    result = {"full_row_duplicates": int(full_row_dupes)}
    if key_cols:
        key_dupes = df.duplicated(subset=key_cols).sum()
        result["key_duplicates"] = int(key_dupes)
        result["key_columns"] = key_cols
    return result


def profile_outliers(df: pd.DataFrame, name: str, output_dir: str, max_hist_cols: int = 12) -> pd.DataFrame:
    """IQR-based outlier detection on numeric columns. Also saves a
    histogram grid for the first `max_hist_cols` numeric columns (this is
    the 'histogram for missing values / distributions' deliverable)."""
    numeric_df = df.select_dtypes(include=[np.number])
    rows = []
    for col in numeric_df.columns:
        series = numeric_df[col].dropna()
        if series.empty:
            continue
        q1, q3 = series.quantile([0.25, 0.75])
        iqr = q3 - q1
        lower, upper = q1 - 1.5 * iqr, q3 + 1.5 * iqr
        outlier_count = ((series < lower) | (series > upper)).sum()
        rows.append({
            "column": col,
            "q1": q1, "q3": q3, "iqr": iqr,
            "lower_bound": lower, "upper_bound": upper,
            "outlier_count": int(outlier_count),
            "outlier_pct": round(outlier_count / len(series) * 100, 2),
        })
    outlier_report = pd.DataFrame(rows).sort_values("outlier_pct", ascending=False)

    hist_cols = numeric_df.columns[:max_hist_cols]
    if len(hist_cols) > 0:
        n_cols = 3
        n_rows = int(np.ceil(len(hist_cols) / n_cols))
        fig, axes = plt.subplots(n_rows, n_cols, figsize=(4 * n_cols, 3 * n_rows))
        axes = np.array(axes).flatten()
        for ax, col in zip(axes, hist_cols):
            numeric_df[col].dropna().hist(ax=ax, bins=30)
            ax.set_title(col, fontsize=9)
        for ax in axes[len(hist_cols):]:
            ax.axis("off")
        plt.suptitle(f"Distributions -- {name}")
        plt.tight_layout()
        plt.savefig(os.path.join(output_dir, f"{name}_histograms.png"))
        plt.close()

    return outlier_report


def is_s3_path(path: str) -> bool:
    return path.startswith("s3://")


def list_csv_sources(input_dir: str) -> list[tuple[str, str]]:
    """Returns a list of (filename, source) pairs, where source is either a
    local filepath or an 's3://bucket/key' string -- profile_file() below
    doesn't care which, since read_csv_source() handles both."""
    if is_s3_path(input_dir):
        parsed = urlparse(input_dir)
        bucket = parsed.netloc
        prefix = parsed.path.lstrip("/")
        if prefix and not prefix.endswith("/"):
            prefix += "/"

        s3 = boto3.client("s3")
        paginator = s3.get_paginator("list_objects_v2")
        sources = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.lower().endswith(".csv"):
                    sources.append((os.path.basename(key), f"s3://{bucket}/{key}"))
        return sources

    return [
        (f, os.path.join(input_dir, f))
        for f in os.listdir(input_dir)
        if f.lower().endswith(".csv")
    ]


def read_csv_source(source: str) -> pd.DataFrame:
    """Reads a CSV from either a local path or an s3:// path, straight into
    memory -- an S3 source is never written to local disk first, so large
    files don't need to be downloaded by hand before profiling."""
    if is_s3_path(source):
        parsed = urlparse(source)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket=bucket, Key=key)
        return pd.read_csv(BytesIO(obj["Body"].read()), low_memory=False)

    return pd.read_csv(source, low_memory=False)


def profile_file(filename: str, source: str, output_dir: str) -> None:
    name = os.path.splitext(filename)[0]
    print(f"\n{'=' * 60}\nProfiling: {name}\n{'=' * 60}")

    try:
        df = read_csv_source(source)
    except Exception as e:
        print(f"FAILED to read {source}: {e}")
        return

    print(f"Shape: {df.shape[0]:,} rows x {df.shape[1]} columns")

    missing_report = profile_missing_values(df, name, output_dir)
    print(f"\nColumns with missing values ({len(missing_report)} of {df.shape[1]}):")
    print(missing_report.to_string() if not missing_report.empty else "  (none)")

    key_cols = guess_key_columns(filename, df.columns.tolist())
    dupes = profile_duplicates(df, key_cols)
    print(f"\nDuplicates: {dupes}")

    outlier_report = profile_outliers(df, name, output_dir)
    print(f"\nTop outlier columns (IQR method):")
    print(outlier_report.head(10).to_string(index=False) if not outlier_report.empty else "  (no numeric columns)")

    # Save the text report alongside the plots
    with open(os.path.join(output_dir, f"{name}_report.txt"), "w") as f:
        f.write(f"Shape: {df.shape}\n\n")
        f.write("MISSING VALUES\n")
        f.write(missing_report.to_string())
        f.write(f"\n\nDUPLICATES\n{dupes}\n\n")
        f.write("OUTLIERS (IQR method)\n")
        f.write(outlier_report.to_string(index=False))


def main():
    parser = argparse.ArgumentParser(description="EDA for healthcare metrics project CSVs")
    parser.add_argument(
        "--input-dir",
        required=True,
        help="Local directory of downloaded CSVs, OR an s3://bucket/prefix path "
        "(e.g. s3://de-healthcare-project-bucket/raw) to read straight from S3",
    )
    parser.add_argument("--output-dir", default="./eda_output", help="Where to save reports/plots")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    csv_sources = list_csv_sources(args.input_dir)

    if not csv_sources:
        print(f"No CSV files found in {args.input_dir}")
        return

    print(f"Found {len(csv_sources)} CSV file(s): {[name for name, _ in csv_sources]}")
    for filename, source in csv_sources:
        profile_file(filename, source, args.output_dir)

    print(f"\nAll reports and plots saved to {args.output_dir}/")


if __name__ == "__main__":
    main()
