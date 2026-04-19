# =============================================================
# src/transformation/bronze_to_silver.py
#
# Purpose : Read raw CSV from MinIO Bronze, apply schema
#           enforcement + data quality rules, write clean
#           Parquet back to MinIO Silver.
#
# Why Parquet over CSV?
#   ┌─────────────────┬──────────────┬────────────────┐
#   │ Format          │ Size (1.6GB) │ Read Time (est)│
#   ├─────────────────┼──────────────┼────────────────┤
#   │ CSV (raw)       │ 1.6 GB       │ ~45s           │
#   │ Parquet+Snappy  │ ~320 MB      │ ~8s            │
#   └─────────────────┴──────────────┴────────────────┘
#   Parquet = columnar storage → only reads columns needed.
#   Snappy  = fast codec, splittable across executors.
#
# Silver Layer Contract (what downstream MUST be able to trust):
#   ✅ MSNDATE is a proper DateType (not string)
#   ✅ Coordinates are DoubleType, nulls for invalid values
#   ✅ NUMWEAPONSDELIVERED is IntegerType, nulls for negatives
#   ✅ Text columns are trimmed & upper-cased (normalised)
#   ✅ Duplicate rows are removed
#   ✅ Data is partitioned by year & month on disk
# =============================================================

import logging
import sys
from pathlib import Path
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, DateType
)

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.utils.spark_session import get_spark_session

logger = logging.getLogger(__name__)

# =============================================================
# Configuration
# =============================================================
BRONZE_BUCKET = "thor-bronze"
SILVER_BUCKET = "thor-silver"
BRONZE_PATH   = f"s3a://{BRONZE_BUCKET}/vietnam_bombing/ingestion_date=2024-01-15/"
SILVER_PATH   = f"s3a://{SILVER_BUCKET}/vietnam_bombing_clean/"

# Columns we actually need — projection pushdown.
# Selecting only required columns avoids deserialising ~50 unused
# columns from the raw CSV, reducing shuffle data volume.
REQUIRED_COLUMNS = [
    "MSNDATE",
    "MILSERVICE",
    "AIRCRAFT_ROOT",
    "TAKEOFFLOCATION",
    "TGTCOUNTRY",
    "TGTTYPE",
    "TGTLATDD_DDD_WGS84",
    "TGTLONDDD_DDD_WGS84",
    "NUMWEAPONSDELIVERED",
    "WEAPONTYPECLASS",
    "TIMEONTARGET",
]


# =============================================================
# Schema Definition
# =============================================================
def get_bronze_schema() -> StructType:
    """
    Explicit schema for the raw THOR CSV.

    Why define schema explicitly instead of inferSchema=True?
      - inferSchema scans the ENTIRE file twice (once to infer,
        once to read) → 2x I/O cost on a 1.6 GB file.
      - Explicit schema guarantees correct types from the start.
      - Schema mismatches fail fast with clear errors.
    """
    return StructType([
        StructField("MSNDATE",               StringType(),  True),
        StructField("MILSERVICE",            StringType(),  True),
        StructField("AIRCRAFT_ROOT",                StringType(),  True),
        StructField("TAKEOFFLOCATION",       StringType(),  True),
        StructField("TGTCOUNTRY",            StringType(),  True),
        StructField("TGTTYPE",               StringType(),  True),
        StructField("TGTLATDD_DDD_WGS84",   StringType(),  True),
        StructField("TGTLONDDD_DDD_WGS84",  StringType(),  True),
        StructField("NUMWEAPONSDELIVERED",   StringType(),  True),
        StructField("WEAPONTYPECLASS",       StringType(),  True),
        StructField("TIMEONTARGET",          StringType(),  True),
    ])


# =============================================================
# Transformation Steps
# =============================================================

def read_bronze(spark: SparkSession) -> DataFrame:
    """Read raw CSV from Bronze with explicit schema."""
    logger.info(f"Reading Bronze layer from: {BRONZE_PATH}")

    df = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")     # Don't crash on malformed rows
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        #.schema(get_bronze_schema())
        .csv(BRONZE_PATH)
        .select(REQUIRED_COLUMNS)         # Early projection — drop unused cols NOW
    )

    raw_count = df.count()
    logger.info(f"Bronze records read: {raw_count:,}")
    return df


def enforce_types_and_clean(df: DataFrame) -> DataFrame:
    """
    Apply type casting, null handling, and text normalisation.

    Rules:
      - MSNDATE       → DateType (format: MM/DD/YYYY)
      - Coordinates   → DoubleType, nullify out-of-range values
      - Weapons count → IntegerType, nullify negatives (data error)
      - String cols   → TRIM + UPPER (uniform comparison in Gold)
    """
    logger.info("Applying type enforcement and cleaning...")

    df_cleaned = (
        df
        # ── Date Parsing ────────────────────────────────────────
        # to_date handles the raw MM/DD/YYYY format in THOR data.
        # Rows with unparseable dates become null → filtered below.
        .withColumn("mission_date",
            F.coalesce(
                F.to_date(F.col("MSNDATE"), "MM/dd/yyyy"),
                F.to_date(F.col("MSNDATE"), "yyyy-MM-dd"),
                F.to_date(F.col("MSNDATE"), "yyyyMMdd") # Phòng trường hợp có dữ liệu dính liền
            )
        )

        # ── Derive Year & Month ─────────────────────────────────
        # These columns serve DUAL purpose:
        #   1. Enable partition pruning when Silver is written
        #      partitioned by year/month.
        #   2. Pre-computed for Gold aggregations (no re-parse).
        .withColumn("mission_year",  F.year("mission_date"))
        .withColumn("mission_month", F.month("mission_date"))

        # ── Coordinate Casting ──────────────────────────────────
        # Cast to Double; strings/blanks → null automatically.
        # Validate: lat ∈ [-90, 90], lon ∈ [-180, 180]
        # Vietnam bounding box: lat 8–24, lon 102–110
        # We apply a broad world-range check to catch clear errors
        # without over-filtering (some ops were outside Vietnam).
        .withColumn("target_lat",
            F.when(
                F.col("TGTLATDD_DDD_WGS84").cast(DoubleType()).between(-90, 90),
                F.col("TGTLATDD_DDD_WGS84").cast(DoubleType())
            ).otherwise(F.lit(None).cast(DoubleType())))

        .withColumn("target_lon",
            F.when(
                F.col("TGTLONDDD_DDD_WGS84").cast(DoubleType()).between(-180, 180),
                F.col("TGTLONDDD_DDD_WGS84").cast(DoubleType())
            ).otherwise(F.lit(None).cast(DoubleType())))

        # ── Weapons Count ───────────────────────────────────────
        # Negative values exist in raw data (recording errors).
        # Business rule: negative count → treat as null.
        .withColumn("num_weapons_delivered",
            F.when(
                F.col("NUMWEAPONSDELIVERED").cast(IntegerType()) >= 0,
                F.col("NUMWEAPONSDELIVERED").cast(IntegerType())
            ).otherwise(F.lit(None).cast(IntegerType())))

        # ── String Normalisation ────────────────────────────────
        # TRIM removes leading/trailing whitespace.
        # UPPER ensures "usaf" == "USAF" == "Usaf" in joins.
        .withColumn("mil_service",
            F.upper(F.trim(F.col("MILSERVICE"))))
        .withColumn("aircraft_type",
            F.upper(F.trim(F.col("AIRCRAFT_ROOT"))))
        .withColumn("takeoff_location",
            F.upper(F.trim(F.col("TAKEOFFLOCATION"))))
        .withColumn("target_country",
            F.upper(F.trim(F.col("TGTCOUNTRY"))))
        .withColumn("target_type",
            F.upper(F.trim(F.col("TGTTYPE"))))
        .withColumn("weapon_type_class",
            F.upper(F.trim(F.col("WEAPONTYPECLASS"))))
        .withColumn("time_on_target",
            F.trim(F.col("TIMEONTARGET")))

        # ── Drop Raw Columns (replaced by clean versions) ───────
        .drop(
            "MSNDATE", "MILSERVICE", "AIRCRAFT_ROOT", "TAKEOFFLOCATION",
            "TGTCOUNTRY", "TGTTYPE", "TGTLATDD_DDD_WGS84",
            "TGTLONDDD_DDD_WGS84", "NUMWEAPONSDELIVERED",
            "WEAPONTYPECLASS", "TIMEONTARGET",
        )
    )

    return df_cleaned


def apply_quality_filters(df: DataFrame) -> DataFrame:
    """
    Drop rows that violate data quality thresholds.

    Filters applied:
      1. mission_date is not null  (unparseable dates → useless)
      2. mission_year in [1964, 1975] (Vietnam War period only)
      3. Deduplicate on all columns
    """
    logger.info("Applying data quality filters...")

    before_count = df.count()

    df_filtered = (
        df
        .filter(F.col("mission_date").isNotNull())
        .filter(F.col("mission_year").between(1964, 1975))
        # cache before dedup because count() + dropDuplicates
        # would trigger two full scans otherwise
        .dropDuplicates()
    )

    after_count = df_filtered.count()
    dropped = before_count - after_count
    logger.info(
        f"Quality filter: {before_count:,} → {after_count:,} rows "
        f"({dropped:,} dropped, {dropped/before_count*100:.2f}%)"
    )
    return df_filtered


def write_silver(df: DataFrame) -> None:
    """
    Write cleaned DataFrame to Silver as partitioned Parquet.

    Partitioning Strategy:
      partitionBy("mission_year", "mission_month")

    Why?
      - Gold queries almost always filter by year/month.
      - Parquet partition pruning skips entire folders,
        so a query for "1968" reads only ~1/12 the data.
      - File layout mirrors what you'd do on AWS S3/Glue.

    repartition(12) before write:
      - 12 = 12 months (1 file per partition target).
      - Avoids the "many tiny files" problem that kills
        downstream read performance.
      - Rule of thumb: target ~128-256 MB per Parquet file.
    """
    logger.info(f"Writing Silver layer to: {SILVER_PATH}")

    (
        df
        # Repartition by the partition key BEFORE writing.
        # This ensures each Parquet file sits cleanly within
        # one year-month partition (no cross-partition files).
        .repartition(12, "mission_year", "mission_month")
        .write
        .mode("overwrite")
        .partitionBy("mission_year", "mission_month")
        .parquet(SILVER_PATH)
    )

    logger.info("✅ Silver layer written successfully.")


# =============================================================
# Orchestration Entry Point
# =============================================================
def run_bronze_to_silver():
    """Full Bronze → Silver pipeline entry point."""
    start = datetime.now(timezone.utc)
    logger.info("=" * 60)
    logger.info("Starting Bronze → Silver transformation")
    logger.info("=" * 60)

    spark = get_spark_session(app_name="THOR-Bronze-to-Silver")

    try:
        df_raw     = read_bronze(spark)
        df_typed   = enforce_types_and_clean(df_raw)
        df_clean   = apply_quality_filters(df_typed)
        write_silver(df_clean)

        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        logger.info(f"Bronze → Silver complete in {elapsed:.1f}s ✅")

    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )
    run_bronze_to_silver()