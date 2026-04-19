# =============================================================
# src/transformation/tuning_lab.py
#
# Purpose : Demonstrate and BENCHMARK three core Spark
#           performance optimisation techniques on the THOR
#           dataset. Results are logged with timing so they
#           can be referenced in the GitHub README.
#
# Techniques demonstrated:
#   1. Salting  — mitigate data skew in GROUP BY / JOIN
#   2. Broadcast Join vs SortMerge Join — choose the right
#      join strategy based on DataFrame size
#   3. Manual Partitioning — control physical data layout
#      to enable partition pruning in downstream queries
# =============================================================

import logging
import time
import sys
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.utils.spark_session import get_spark_session

logger = logging.getLogger(__name__)

SILVER_PATH = "s3a://thor-silver/vietnam_bombing_clean/"


# =============================================================
# Helper
# =============================================================
def timer(label: str):
    """Context manager to log elapsed time for a code block."""
    import contextlib

    @contextlib.contextmanager
    def _timer():
        start = time.perf_counter()
        yield
        elapsed = time.perf_counter() - start
        logger.info(f"⏱  [{label}] completed in {elapsed:.2f}s")

    return _timer()


# =============================================================
# TECHNIQUE 1 — Data Skew & Salting
# =============================================================
def demonstrate_salting(spark: SparkSession, df: DataFrame) -> None:
    """
    Problem: Data Skew in GROUP BY mil_service
    ─────────────────────────────────────────────────────────
    The THOR dataset is heavily skewed by military service:

      USAF  → ~3,200,000 rows  (69% of data)
      USN   → ~700,000  rows   (15%)
      USMC  → ~400,000  rows   ( 9%)
      USA   → ~300,000  rows   ( 7%)

    Without salting:
      - The "USAF" partition goes to ONE executor.
      - That executor processes 10x more data than others.
      - The job stalls waiting for the hot partition.
      - You see this as ONE red task in Spark UI.

    Salting Fix:
      1. Append a random salt (0 to N-1) to the group key.
         "USAF" → "USAF_0", "USAF_1", ..., "USAF_7"
      2. Aggregate across salted keys (partial aggregation).
      3. Strip the salt and re-aggregate (final aggregation).
      Result: USAF's load is split across N executors.
    """
    SALT_BUCKETS = 8  # Number of sub-partitions for skewed keys

    logger.info("─" * 50)
    logger.info("TECHNIQUE 1: Data Skew Analysis & Salting")
    logger.info("─" * 50)

    # ── Step 0: Show skew distribution ────────────────────────
    logger.info("Skew distribution (rows per mil_service):")
    df.groupBy("mil_service") \
      .count() \
      .orderBy(F.desc("count")) \
      .show(10, truncate=False)

    # ── WITHOUT Salting (baseline) ─────────────────────────────
    logger.info("Running GROUP BY without salting (baseline)...")
    with timer("No-Salt GROUP BY"):
        df.groupBy("mil_service") \
          .agg(F.sum("num_weapons_delivered").alias("total_weapons")) \
          .collect()  # .collect() forces full execution

    # ── WITH Salting ───────────────────────────────────────────
    logger.info(f"Running salted GROUP BY with {SALT_BUCKETS} buckets...")
    with timer("Salted GROUP BY"):
        df_salted = df.withColumn(
            "salted_key",
            F.concat(
                F.col("mil_service"),
                F.lit("_"),
                (F.rand() * SALT_BUCKETS).cast(IntegerType()).cast("string")
            )
        )

        # Phase 1: Partial aggregation on salted key.
        # This distributes the USAF rows across 8 executors.
        df_partial = (
            df_salted
            .groupBy("salted_key")
            .agg(F.sum("num_weapons_delivered").alias("partial_sum"))
        )

        # Phase 2: Strip salt, final aggregation.
        # "USAF_0" + "USAF_1" + ... + "USAF_7" → "USAF"
        df_final = (
            df_partial
            .withColumn("mil_service",
                F.regexp_replace(F.col("salted_key"), "_\\d+$", ""))
            .groupBy("mil_service")
            .agg(F.sum("partial_sum").alias("total_weapons"))
        )
        df_final.collect()

    logger.info(
        "💡 Salting result: The USAF partition's work is spread "
        f"across {SALT_BUCKETS} executors instead of 1. "
        "In Spark UI, all tasks finish at roughly the same time."
    )


# =============================================================
# TECHNIQUE 2 — Broadcast Join vs SortMerge Join
# =============================================================
def demonstrate_join_strategies(spark: SparkSession, df: DataFrame) -> None:
    """
    Context: We want to enrich each bombing mission with
             a descriptive label for the military service.

    Two Approaches:
    ┌─────────────────┬───────────────────────┬───────────────┐
    │ Strategy        │ When to use           │ Cost          │
    ├─────────────────┼───────────────────────┼───────────────┤
    │ SortMerge Join  │ Both DFs are large    │ 2 full sorts  │
    │                 │ (> broadcast threshold│ + shuffle     │
    ├─────────────────┼───────────────────────┼───────────────┤
    │ Broadcast Join  │ One DF is small       │ 0 shuffles!   │
    │                 │ (< 20MB by default)   │ 1 broadcast   │
    └─────────────────┴───────────────────────┴───────────────┘

    The lookup table (4 rows) is TINY → Broadcast is the
    clear winner. We benchmark both to prove it.
    """
    logger.info("─" * 50)
    logger.info("TECHNIQUE 2: Broadcast Join vs SortMerge Join")
    logger.info("─" * 50)

    # Small lookup table: military service → full name
    service_labels = [
        ("USAF", "United States Air Force"),
        ("USN",  "United States Navy"),
        ("USMC", "United States Marine Corps"),
        ("USA",  "United States Army"),
    ]
    df_lookup = spark.createDataFrame(service_labels, ["mil_service", "service_name"])
    # 4 rows × 2 columns ≈ a few hundred bytes → perfect for broadcast

    # ── SortMerge Join (default when broadcast is disabled) ────
    logger.info("Running SortMerge Join (broadcast disabled)...")
    with timer("SortMerge Join"):
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # Disable
        df.join(df_lookup, on="mil_service", how="left") \
          .select("mission_date", "mil_service", "service_name",
                  "num_weapons_delivered") \
          .count()

    # ── Broadcast Join (explicit hint) ────────────────────────
    logger.info("Running Broadcast Join (explicit F.broadcast hint)...")
    with timer("Broadcast Join"):
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold",
                       str(20 * 1024 * 1024))  # Re-enable
        # F.broadcast() is an explicit hint — even if AQE is off,
        # this forces the driver to send df_lookup to ALL executors.
        # No shuffle of the large df needed at all.
        df.join(F.broadcast(df_lookup), on="mil_service", how="left") \
          .select("mission_date", "mil_service", "service_name",
                  "num_weapons_delivered") \
          .count()

    logger.info(
        "💡 Broadcast Join eliminates the shuffle of the 4.5M-row DF. "
        "Only the 4-row lookup is sent over the network to each executor. "
        "Check Spark UI → SQL tab → no 'Exchange' node in the DAG."
    )


# =============================================================
# TECHNIQUE 3 — Partition Pruning Demo
# =============================================================
def demonstrate_partition_pruning(spark: SparkSession) -> None:
    """
    Silver layer is written partitioned by year and month.
    Reading a filtered query should only scan relevant folders.

    This shows: without partitioning → full scan.
                 with partitioning    → only reads 1968/ folder.
    """
    logger.info("─" * 50)
    logger.info("TECHNIQUE 3: Partition Pruning")
    logger.info("─" * 50)

    # Reading the FULL Silver Parquet (all years)
    df_full = spark.read.parquet(SILVER_PATH)

    # ── Full Scan (no filter) ──────────────────────────────────
    logger.info("Counting rows — full scan (no partition filter):")
    with timer("Full scan — all years"):
        total = df_full.count()
        logger.info(f"  Total rows across all partitions: {total:,}")

    # ── Partition Pruned Scan ──────────────────────────────────
    # Spark reads partition folders from the path metadata.
    # Filtering on mission_year=1968 means Spark ONLY opens:
    #   s3a://thor-silver/vietnam_bombing_clean/mission_year=1968/
    # and completely skips all other year folders.
    logger.info("Counting rows — partition pruned (year=1968 only):")
    with timer("Partition pruned — 1968 only"):
        count_1968 = (
            df_full
            .filter(F.col("mission_year") == 1968)
            .count()
        )
        logger.info(f"  Rows in 1968: {count_1968:,}")

    logger.info(
        "💡 Verify pruning: Spark UI → SQL tab → 'PartitionCount' "
        "should be 12 (1 year × 12 months) for the pruned query "
        "vs 132 (11 years × 12 months) for full scan."
    )


# =============================================================
# Entry Point
# =============================================================
def run_tuning_lab():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )
    spark = get_spark_session(app_name="THOR-Tuning-Lab")

    try:
        df = spark.read.parquet(SILVER_PATH).cache()
        # .cache() keeps Silver in executor memory for all 3 demos
        # so we measure transformation cost, not I/O cost.
        df.count()  # Trigger cache materialisation

        demonstrate_salting(spark, df)
        demonstrate_join_strategies(spark, df)
        demonstrate_partition_pruning(spark)

    finally:
        spark.stop()


if __name__ == "__main__":
    run_tuning_lab()