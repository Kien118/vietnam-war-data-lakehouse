# =============================================================
# src/ingestion/upload_to_minio.py
#
# Purpose : Ingest raw THOR CSV files from local disk into the
#           MinIO Bronze bucket — the first layer of our
#           Medallion Architecture.
#
# Design Decisions:
#   1. Idempotent  : Re-running will NOT duplicate data.
#                    We check if the object already exists
#                    before uploading (skip if found).
#   2. Partitioned : Objects are stored with a Hive-style
#                    partition path so PySpark can read them
#                    efficiently later.
#                    Path pattern:
#                    thor-bronze/
#                    └── vietnam_bombing/
#                        └── ingestion_date=2024-01-15/
#                            └── THOR_Vietnam_Bombing_Operations.csv
#   3. Metadata    : Each uploaded object carries S3 metadata
#                    tags for lineage tracking.
# =============================================================

import os
import sys
import logging
import argparse
from pathlib import Path
from datetime import datetime, timezone

from minio.error import S3Error

# Ensure src/ is importable when running as a script
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.utils.minio_client import get_minio_client, ensure_bucket_exists

# =============================================================
# Logging Configuration
# =============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ingestion.upload_to_minio")


# =============================================================
# Constants
# =============================================================
BRONZE_BUCKET = os.getenv("MINIO_BRONZE_BUCKET", "thor-bronze")
DATASET_NAME  = "vietnam_bombing"          # Logical dataset folder


# =============================================================
# Core Functions
# =============================================================

def build_object_key(filename: str, ingestion_date: str) -> str:
    """
    Build a Hive-partitioned S3 object key for the uploaded file.

    Pattern: <dataset>ingestion_date=<date>/<filename>

    Why partition by ingestion_date?
      - Enables PySpark partition pruning when reading Bronze.
      - Creates a clear audit trail of when data arrived.
      - Mirrors best practices from production Data Lakes
        (AWS S3, Azure ADLS, GCS).

    Args:
        filename       : Original file name (e.g. "THOR_Vietnam.csv").
        ingestion_date : ISO date string (e.g. "2024-01-15").

    Returns:
        str: Full S3 object key.
    """
    return f"{DATASET_NAME}/ingestion_date={ingestion_date}/{filename}"


def object_exists(client, bucket: str, key: str) -> bool:
    """
    Check whether an object already exists in MinIO.
    Used to make the upload step idempotent.

    Args:
        client : MinIO client.
        bucket : Bucket name.
        key    : Object key.

    Returns:
        bool: True if object exists, False otherwise.
    """
    try:
        client.stat_object(bucket, key)
        return True
    except S3Error as e:
        if e.code == "NoSuchKey":
            return False
        raise  # Re-raise unexpected errors


def upload_file_to_bronze(
    local_path: str,
    ingestion_date: str | None = None,
    force: bool = False,
) -> dict:
    """
    Upload a single CSV file to the Bronze MinIO bucket.

    Args:
        local_path     : Absolute or relative path to the local CSV file.
        ingestion_date : Partition date (defaults to today's UTC date).
        force          : If True, overwrite existing objects.

    Returns:
        dict: Upload result metadata for logging/DAG XCom.

    Raises:
        FileNotFoundError : If the local file does not exist.
        S3Error           : On MinIO upload failure.
    """
    local_path = Path(local_path).resolve()

    if not local_path.exists():
        raise FileNotFoundError(f"Source file not found: {local_path}")

    if ingestion_date is None:
        ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    filename   = local_path.name
    object_key = build_object_key(filename, ingestion_date)
    file_size  = local_path.stat().st_size

    logger.info(f"Starting ingestion | file: {filename} | size: {file_size / 1e6:.2f} MB")
    logger.info(f"Target: s3://{BRONZE_BUCKET}/{object_key}")

    client = get_minio_client()
    ensure_bucket_exists(client, BRONZE_BUCKET)

    # ── Idempotency check ──────────────────────────────────────
    if not force and object_exists(client, BRONZE_BUCKET, object_key):
        logger.warning(
            f"Object already exists → SKIPPING (use --force to overwrite): "
            f"s3://{BRONZE_BUCKET}/{object_key}"
        )
        return {
            "status": "skipped",
            "bucket": BRONZE_BUCKET,
            "object_key": object_key,
            "file_size_mb": round(file_size / 1e6, 2),
        }

    # ── Upload ─────────────────────────────────────────────────
    start_time = datetime.now(timezone.utc)

    client.fput_object(
        bucket_name=BRONZE_BUCKET,
        object_name=object_key,
        file_path=str(local_path),
        content_type="text/csv",
        # S3 metadata for data lineage tracking
        metadata={
            "X-Amz-Meta-Source":          "kaggle/usaf/vietnam-bombing-data",
            "X-Amz-Meta-Ingestion-Date":  ingestion_date,
            "X-Amz-Meta-Pipeline":        "thor-data-pipeline",
            "X-Amz-Meta-Layer":           "bronze",
            "X-Amz-Meta-Original-Name":   filename,
        },
    )

    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
    logger.info(
        f"✅ Upload complete | {filename} → s3://{BRONZE_BUCKET}/{object_key} "
        f"| {file_size / 1e6:.2f} MB in {elapsed:.1f}s "
        f"| Speed: {(file_size / 1e6) / elapsed:.1f} MB/s"
    )

    return {
        "status": "uploaded",
        "bucket": BRONZE_BUCKET,
        "object_key": object_key,
        "file_size_mb": round(file_size / 1e6, 2),
        "elapsed_seconds": round(elapsed, 2),
        "ingestion_date": ingestion_date,
    }


def run_ingestion(data_dir: str, ingestion_date: str | None = None, force: bool = False):
    """
    Scan a local directory and upload all CSV files to Bronze.

    Args:
        data_dir       : Directory containing raw CSV files.
        ingestion_date : Optional date override.
        force          : Overwrite existing objects if True.
    """
    data_dir = Path(data_dir).resolve()

    if not data_dir.exists():
        raise FileNotFoundError(f"Data directory not found: {data_dir}")

    csv_files = list(data_dir.glob("*.csv"))

    if not csv_files:
        logger.warning(f"No CSV files found in: {data_dir}")
        return

    logger.info(f"Found {len(csv_files)} CSV file(s) to ingest from: {data_dir}")

    results = []
    for csv_file in csv_files:
        try:
            result = upload_file_to_bronze(
                local_path=str(csv_file),
                ingestion_date=ingestion_date,
                force=force,
            )
            results.append(result)
        except Exception as e:
            logger.error(f"Failed to upload {csv_file.name}: {e}", exc_info=True)

    # ── Summary ────────────────────────────────────────────────
    uploaded = [r for r in results if r["status"] == "uploaded"]
    skipped  = [r for r in results if r["status"] == "skipped"]
    total_mb = sum(r["file_size_mb"] for r in uploaded)

    logger.info("=" * 60)
    logger.info(f"Ingestion Summary:")
    logger.info(f"  ✅ Uploaded : {len(uploaded)} file(s) | {total_mb:.2f} MB total")
    logger.info(f"  ⏭️  Skipped  : {len(skipped)} file(s) (already in Bronze)")
    logger.info("=" * 60)

    return results


# =============================================================
# CLI Entry Point
# =============================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Ingest THOR raw CSV files into MinIO Bronze bucket."
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default="./data/raw",
        help="Path to directory containing raw CSV files (default: ./data/raw)",
    )
    parser.add_argument(
        "--ingestion-date",
        type=str,
        default=None,
        help="Partition date override in YYYY-MM-DD format (default: today UTC)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-upload even if object already exists in Bronze",
    )

    args = parser.parse_args()

    run_ingestion(
        data_dir=args.data_dir,
        ingestion_date=args.ingestion_date,
        force=args.force,
    )