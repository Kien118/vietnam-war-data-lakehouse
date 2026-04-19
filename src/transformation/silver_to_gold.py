# =============================================================
# src/transformation/silver_to_gold.py
#
# Purpose : Read Silver Parquet, compute business aggregations,
#           write Gold tables ready for Power BI consumption.
#
# Gold Tables produced:
#   gold_missions_by_year_service
#     → Total missions & weapons per year × military service
#     → Powers: yearly trend line chart
#
#   gold_top_aircraft
#     → Top aircraft types by mission count
#     → Powers: bar chart ranked by usage
#
#   gold_bombing_intensity_by_country
#     → Total weapons delivered per target country per year
#     → Powers: heatmap / choropleth map
#
#   gold_monthly_ops_trend
#     → Monthly operation volume across the war
#     → Powers: time series / area chart
#
# Why write Gold to PostgreSQL (not Parquet)?
#   Power BI Desktop (free) cannot query S3/MinIO directly.
#   PostgreSQL is the standard OLAP target for local BI tools.
#   In production: Gold would go to Redshift / BigQuery.
# =============================================================

import logging
import sys
from pathlib import Path
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.utils.spark_session import get_spark_session

logger = logging.getLogger(__name__)

SILVER_PATH  = "s3a://thor-silver/vietnam_bombing_clean/"

# PostgreSQL connection (Spark JDBC)
PG_HOST     = "localhost"
PG_PORT     = "5432"
PG_DB       = "thor_warehouse"
PG_USER     = "thor_admin"
PG_PASSWORD = "thor_secure_pass_2025"
PG_URL      = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
PG_DRIVER   = "org.postgresql.Driver"


def write_to_postgres(df: DataFrame, table_name: str, mode: str = "overwrite") -> None:
    """
    Write a DataFrame to PostgreSQL via Spark JDBC.

    Args:
        df         : DataFrame to write.
        table_name : Target PostgreSQL table name.
        mode       : Write mode (overwrite / append).
    """
    logger.info(f"Writing {df.count():,} rows → PostgreSQL table: {table_name}")

    (
        df.write
        .format("jdbc")
        .option("url",      PG_URL)
        .option("dbtable",  table_name)
        .option("user",     PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver",   PG_DRIVER)
        # batchsize: how many rows per INSERT batch.
        # Default is 1000; 10000 is faster for bulk loads.
        .option("batchsize", "10000")
        # numPartitions: parallel JDBC writers.
        # Match to PostgreSQL max_connections limit.
        .option("numPartitions", "4")
        .mode(mode)
        .save()
    )
    logger.info(f"✅ {table_name} written to PostgreSQL")


def build_gold_missions_by_year_service(df: DataFrame) -> DataFrame:
    """
    Aggregate: Total missions and weapons per year × mil_service.
    Used for: Yearly trend analysis by military branch.
    """
    return (
        df
        .groupBy("mission_year", "mil_service")
        .agg(
            F.count("*").alias("total_missions"),
            F.sum("num_weapons_delivered").alias("total_weapons_delivered"),
            F.avg("num_weapons_delivered").alias("avg_weapons_per_mission"),
            F.countDistinct("takeoff_location").alias("unique_bases_used"),
        )
        .orderBy("mission_year", "mil_service")
    )


def build_gold_top_aircraft(df: DataFrame) -> DataFrame:
    """
    Aggregate: Mission count and total weapons by aircraft type.
    Used for: Ranking most-used aircraft of the Vietnam War.
    """
    return (
        df
        .filter(F.col("aircraft_type").isNotNull())
        .groupBy("aircraft_type")
        .agg(
            F.count("*").alias("total_missions"),
            F.sum("num_weapons_delivered").alias("total_weapons_delivered"),
            F.countDistinct("mil_service").alias("services_operated"),
        )
        .orderBy(F.desc("total_missions"))
        .limit(30)  # Top 30 aircraft for BI readability
    )


def build_gold_bombing_intensity_by_country(df: DataFrame) -> DataFrame:
    """
    Aggregate: Weapons delivered per target country per year.
    Used for: Geographic intensity heatmap in Power BI.
    """
    return (
        df
        .filter(F.col("target_country").isNotNull())
        .groupBy("mission_year", "target_country")
        .agg(
            F.count("*").alias("total_missions"),
            F.sum("num_weapons_delivered").alias("total_weapons_delivered"),
        )
        .orderBy("mission_year", F.desc("total_weapons_delivered"))
    )


def build_gold_monthly_ops_trend(df: DataFrame) -> DataFrame:
    """
    Aggregate: Monthly mission volume across the entire war.
    Used for: Time series trend line showing operational tempo.
    """
    return (
        df
        .groupBy("mission_year", "mission_month")
        .agg(
            F.count("*").alias("total_missions"),
            F.sum("num_weapons_delivered").alias("total_weapons_delivered"),
            F.countDistinct("aircraft_type").alias("aircraft_types_used"),
        )
        # Create a proper date column for Power BI time axis
        .withColumn(
            "year_month",
            F.to_date(
                F.concat_ws("-",
                    F.col("mission_year"),
                    F.lpad(F.col("mission_month").cast("string"), 2, "0"),
                    F.lit("01")
                ),
                "yyyy-MM-dd"
            )
        )
        .orderBy("mission_year", "mission_month")
    )


def run_silver_to_gold():
    """Full Silver → Gold pipeline entry point."""
    start = datetime.now(timezone.utc)
    logger.info("=" * 60)
    logger.info("Starting Silver → Gold transformation")
    logger.info("=" * 60)

    spark = get_spark_session(app_name="THOR-Silver-to-Gold")

    try:
        # ── Read Silver ────────────────────────────────────────
        # Cache Silver since we scan it 4 times (one per Gold table).
        # Without cache: 4 × full S3 reads = wasted I/O.
        logger.info("Reading & caching Silver layer...")
        df_silver = spark.read.parquet(SILVER_PATH).cache()
        silver_count = df_silver.count()  # Materialise cache
        logger.info(f"Silver rows cached: {silver_count:,}")

        # ── Build & Write Gold Tables ──────────────────────────
        gold_tables = {
            "gold_missions_by_year_service":     build_gold_missions_by_year_service(df_silver),
            "gold_top_aircraft":                 build_gold_top_aircraft(df_silver),
            "gold_bombing_intensity_by_country": build_gold_bombing_intensity_by_country(df_silver),
            "gold_monthly_ops_trend":            build_gold_monthly_ops_trend(df_silver),
        }

        for table_name, df_gold in gold_tables.items():
            write_to_postgres(df_gold, table_name)

        elapsed = (datetime.now(timezone.utc) - start).total_seconds()
        logger.info("=" * 60)
        logger.info(f"Silver → Gold complete in {elapsed:.1f}s ✅")
        logger.info(f"Gold tables written: {list(gold_tables.keys())}")
        logger.info("=" * 60)

    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )
    run_silver_to_gold()