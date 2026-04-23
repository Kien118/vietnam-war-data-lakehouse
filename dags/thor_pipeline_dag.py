# =============================================================
# dags/thor_pipeline_dag.py
#
# Purpose : Orchestrate the full THOR Medallion Pipeline.
#           Bronze (MinIO CSV) → Silver (Parquet) → Gold (PostgreSQL)
#
# Design Principles applied:
#   1. Idempotency   : Each task can be re-run safely.
#                      Re-runs use the same logical_date so
#                      MinIO paths & PG writes are deterministic.
#   2. Atomicity     : Validate gates between layers prevent
#                      bad data propagating downstream.
#   3. Observability : Each task logs row counts & timings.
#                      Failures include actionable error context.
#   4. Parallelism   : Gold aggregation tasks run concurrently
#                      (no data dependency between them).
#
# Airflow Concepts demonstrated:
#   - TaskFlow API (@task decorator) — cleaner than operators
#   - Dynamic task dependencies
#   - XCom for passing metadata between tasks
#   - trigger_rule for fan-in after parallel tasks
#   - Jinja templating for logical_date partition paths
# =============================================================

from __future__ import annotations

import logging
import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowFailException

# Ensure src/ on path when running inside Airflow container
sys.path.insert(0, "/opt/airflow/src")

logger = logging.getLogger(__name__)

# =============================================================
# DAG Default Arguments
# =============================================================
DEFAULT_ARGS = {
    "owner":            "thor-data-engineering",
    "depends_on_past":  False,
    # Retry once after 5 minutes — handles transient MinIO/Spark issues
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,   # Set True + configure SMTP in production
    "email_on_retry":   False,
}

# =============================================================
# Environment — pulled from Airflow's inherited .env
# =============================================================
# --- Cập nhật lại cho khớp với file .env ---
_raw_minio_endpoint = os.getenv("MINIO_ENDPOINT", "host.docker.internal:9005") 
MINIO_ENDPOINT = _raw_minio_endpoint.replace("http://", "").replace("https://", "")

MINIO_USER       = os.getenv("MINIO_ROOT_USER",      "minio_admin")             
MINIO_PASSWORD   = os.getenv("MINIO_ROOT_PASSWORD",  "minio_secure_pass_2025")  
BRONZE_BUCKET    = os.getenv("MINIO_BRONZE_BUCKET",  "thor-bronze")
SILVER_BUCKET    = os.getenv("MINIO_SILVER_BUCKET",  "thor-silver")
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL",     "spark://spark-master:7077")

PG_HOST     = "postgres"
PG_PORT     = "5432"
PG_DB       = os.getenv("POSTGRES_DB",       "thor_warehouse")
PG_USER     = os.getenv("POSTGRES_USER",     "thor_admin")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "thor_secure_pass_2025")

RAW_DATA_DIR = "/opt/airflow/data/raw"   # Mounted via docker-compose volume

# Minimum row count to pass validation gates
BRONZE_MIN_ROWS = 4_000_000   # We know THOR has ~4.5M rows
SILVER_MIN_ROWS = 4_000_000
GOLD_MIN_ROWS   = {
    "gold_missions_by_year_service":     10,
    "gold_top_aircraft":                 10,
    "gold_bombing_intensity_by_country": 10,
    "gold_monthly_ops_trend":            100,
}


# =============================================================
# DAG Definition
# =============================================================
@dag(
    dag_id="thor_medallion_pipeline",
    description=(
        "End-to-end THOR Vietnam War bombing data pipeline. "
        "Bronze (MinIO CSV) → Silver (Parquet) → Gold (PostgreSQL)."
    ),
    default_args=DEFAULT_ARGS,
    # Run on the 1st of every month.
    # logical_date = the month being processed (not the run date).
    schedule="@monthly",
    start_date=datetime(2024, 1, 1),
    catchup=False,       # Don't backfill historical runs on first deploy
    max_active_runs=1,   # Prevent concurrent runs from clobbering each other
    tags=["thor", "medallion", "pyspark", "minio", "postgres"],
    doc_md="""
## THOR Medallion Pipeline

Orchestrates the full data engineering pipeline for the
**Theater History of Operations Reports (THOR)** Vietnam War dataset.

### Architecture
### Layers
| Layer  | Storage        | Format  | Description              |
|--------|----------------|---------|--------------------------|
| Bronze | MinIO          | CSV     | Raw data, immutable      |
| Silver | MinIO          | Parquet | Cleaned, typed, partitioned |
| Gold   | PostgreSQL     | Tables  | Business aggregations    |

### Triggering manually
```bash
airflow dags trigger thor_medallion_pipeline
```
""",
)
def thor_medallion_pipeline():

    # ── Sentinel tasks (visual anchors in Airflow Graph view) ──
    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(
        task_id="pipeline_complete",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ===========================================================
    # HEALTH CHECKS
    # ===========================================================

    @task(task_id="check_minio_health")
    def check_minio_health() -> dict:
        """
        Verify MinIO is reachable and required buckets exist.

        Why health check first?
          Running a 4.5M-row PySpark job only to discover MinIO
          is down wastes 20+ minutes. Fail fast in <1s instead.

        Returns:
            dict: Health status metadata passed via XCom.
        """
        from minio import Minio
        from minio.error import S3Error

        logger.info(f"Checking MinIO health at: {MINIO_ENDPOINT}")

        client = Minio(
            endpoint=MINIO_ENDPOINT,
            access_key=MINIO_USER,
            secret_key=MINIO_PASSWORD,
            secure=False,
        )

        missing_buckets = []
        for bucket in [BRONZE_BUCKET, SILVER_BUCKET]:
            if not client.bucket_exists(bucket):
                missing_buckets.append(bucket)

        if missing_buckets:
            raise AirflowFailException(
                f"MinIO health check FAILED. "
                f"Missing buckets: {missing_buckets}. "
                f"Run minio-init service to create them."
            )

        logger.info("✅ MinIO health check passed. All buckets exist.")
        return {"status": "healthy", "endpoint": MINIO_ENDPOINT}


    @task(task_id="check_postgres_health")
    def check_postgres_health() -> dict:
        """
        Verify PostgreSQL is reachable and target DB exists.

        Returns:
            dict: Health status metadata.
        """
        import psycopg2

        logger.info(f"Checking PostgreSQL health at {PG_HOST}:{PG_PORT}/{PG_DB}")

        try:
            conn = psycopg2.connect(
                host=PG_HOST, port=PG_PORT, dbname=PG_DB,
                user=PG_USER, password=PG_PASSWORD,
                connect_timeout=10,
            )
            conn.close()
        except psycopg2.OperationalError as e:
            raise AirflowFailException(
                f"PostgreSQL health check FAILED: {e}. "
                "Check that the postgres container is healthy."
            )

        logger.info("✅ PostgreSQL health check passed.")
        return {"status": "healthy", "host": PG_HOST, "db": PG_DB}


    # ===========================================================
    # LAYER 1: INGESTION (Bronze)
    # ===========================================================

    @task(task_id="ingest_raw_to_bronze")
    def ingest_raw_to_bronze(**context) -> dict:
        """
        Upload raw CSV files from local disk to MinIO Bronze bucket.

        Uses logical_date as the ingestion_date partition key.
        This makes every monthly run self-contained and re-runnable:
          logical_date = 2024-01-01 → ingestion_date=2024-01-01

        Returns:
            dict: Ingestion result metadata for downstream validation.
        """
        # Jinja logical_date → partition key (e.g. "2024-01-01")
        logical_date: datetime = context["logical_date"]
        ingestion_date = logical_date.strftime("%Y-%m-%d")

        logger.info(f"Starting Bronze ingestion | partition: {ingestion_date}")

        import sys
        sys.path.append('/opt/airflow')
        # Import here (inside task) so Airflow worker loads it at runtime.
        # This avoids import errors at DAG parse time if src/ not on path.
        from src.ingestion.upload_to_minio import run_ingestion

        results = run_ingestion(
            data_dir=RAW_DATA_DIR,
            ingestion_date=ingestion_date,
            force=False,   # Idempotent: skip if already uploaded
        )

        uploaded = [r for r in results if r["status"] == "uploaded"]
        skipped  = [r for r in results if r["status"] == "skipped"]

        logger.info(
            f"Bronze ingestion done | "
            f"uploaded: {len(uploaded)} | skipped: {len(skipped)}"
        )

        return {
            "ingestion_date":   ingestion_date,
            "files_uploaded":   len(uploaded),
            "files_skipped":    len(skipped),
            "total_size_mb":    sum(r.get("file_size_mb", 0) for r in uploaded),
        }


    @task(task_id="validate_bronze_data")
    def validate_bronze_data(ingestion_meta: dict) -> dict:
        """
        Quality gate: confirm Bronze data landed correctly.

        Checks:
          1. At least one file was uploaded OR skipped (not zero).
          2. Object exists in MinIO at expected path.

        This gate prevents the PySpark job from starting on empty Bronze.
        """
        from minio import Minio

        ingestion_date  = ingestion_meta["ingestion_date"]
        files_processed = (
            ingestion_meta["files_uploaded"] + ingestion_meta["files_skipped"]
        )

        if files_processed == 0:
            raise AirflowFailException(
                "Bronze validation FAILED: No files found in raw data directory. "
                f"Ensure CSV exists at {RAW_DATA_DIR}"
            )

        # Verify the object physically exists in MinIO
        client = Minio(MINIO_ENDPOINT, MINIO_USER, MINIO_PASSWORD, secure=False)
        expected_key = (
            f"vietnam_bombing/ingestion_date={ingestion_date}/"
            "THOR_Vietnam_Bombing_Operations.csv"
        )

        try:
            stat = client.stat_object(BRONZE_BUCKET, expected_key)
            size_mb = stat.size / 1e6
            logger.info(
                f"✅ Bronze validation passed | "
                f"object: {expected_key} | size: {size_mb:.1f} MB"
            )
        except Exception as e:
            raise AirflowFailException(
                f"Bronze validation FAILED: Object not found in MinIO. "
                f"Expected: s3a://{BRONZE_BUCKET}/{expected_key}. Error: {e}"
            )

        return {
            "ingestion_date":   ingestion_date,
            "files_processed":  files_processed,
            "bronze_size_mb":   size_mb,
            "validation":       "passed",
        }


    # ===========================================================
    # LAYER 2: TRANSFORMATION (Silver)
    # ===========================================================

    @task(task_id="transform_bronze_to_silver", execution_timeout=timedelta(hours=1))
    def transform_bronze_to_silver(bronze_meta: dict) -> dict:
        """
        Run PySpark Bronze → Silver transformation.

        execution_timeout=1h: PySpark on 4.5M rows takes ~10-20min
        locally. The timeout gives headroom while preventing a
        hung Spark job from blocking the scheduler indefinitely.

        We invoke the transformation as a subprocess (spark-submit)
        rather than importing PySpark inline. Why?
          - Airflow workers run Python; Spark runs on the Spark cluster.
          - spark-submit properly allocates cluster resources.
          - Avoids JVM classpath conflicts in the Airflow container.

        Returns:
            dict: Transform metadata (row counts, timing).
        """
        ingestion_date = bronze_meta["ingestion_date"]

        logger.info(
            f"Submitting Bronze → Silver Spark job | "
            f"master: {SPARK_MASTER_URL}"
        )

        spark_env = os.environ.copy()
        spark_env["SPARK_HOME"] = "/opt/spark"
        spark_env["PYTHONPATH"] = "/opt/airflow"

        cmd = [
            "/opt/spark/bin/spark-submit",     # <--- Sửa dòng này
            "--master",        SPARK_MASTER_URL,
            "--name",          "THOR-Bronze-to-Silver",
            "--packages",      (
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
                "org.postgresql:postgresql:42.7.1"
            ),
            "--conf", f"spark.hadoop.fs.s3a.endpoint=http://{MINIO_ENDPOINT}",
            "--conf", f"spark.hadoop.fs.s3a.access.key={MINIO_USER}",
            "--conf", f"spark.hadoop.fs.s3a.secret.key={MINIO_PASSWORD}",
            "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
            "--conf", "spark.sql.adaptive.enabled=true",
            "/opt/airflow/src/transformation/bronze_to_silver.py",
        ]

        logger.info(f"spark-submit command: {' '.join(cmd)}")

        result = subprocess.run(
            cmd,
            env=spark_env,
            capture_output=True,
            text=True,
            timeout=3600,   # 60 minutes hard timeout
        )

        if result.returncode != 0:
            logger.error(f"Spark STDOUT:\n{result.stdout[-3000:]}")
            logger.error(f"Spark STDERR:\n{result.stderr[-3000:]}")  # Last 3k chars
            raise AirflowFailException(
                f"Bronze → Silver Spark job FAILED. "
                f"Return code: {result.returncode}. "
                f"Check logs above for Spark error details."
            )

        logger.info("✅ Bronze → Silver Spark job completed successfully.")
        logger.info(f"Spark STDOUT (tail):\n{result.stdout[-1000:]}")

        return {
            "ingestion_date": ingestion_date,
            "silver_path":    "s3a://thor-silver/vietnam_bombing_clean/",
            "status":         "success",
        }


    @task(task_id="validate_silver_data")
    def validate_silver_data(silver_meta: dict) -> dict:
        """
        Quality gate: verify Silver Parquet partitions exist and
        contain the expected minimum row count.

        Uses MinIO client (not Spark) for fast partition existence check.
        Only spins up Spark for row count if partitions exist.
        """
        from minio import Minio

        client = Minio(MINIO_ENDPOINT, MINIO_USER, MINIO_PASSWORD, secure=False)

        # Check at least some year partitions exist in Silver
        silver_prefix = "vietnam_bombing_clean/"
        objects = list(client.list_objects(SILVER_BUCKET, prefix=silver_prefix))

        if not objects:
            raise AirflowFailException(
                f"Silver validation FAILED: No Parquet files found at "
                f"s3a://{SILVER_BUCKET}/{silver_prefix}"
            )

        partition_folders = {
            obj.object_name.split("/")[1]
            for obj in objects
            if "/" in obj.object_name
        }
        logger.info(f"Silver partitions found: {sorted(partition_folders)}")
        logger.info(f"✅ Silver validation passed | {len(objects)} objects found")

        return {
            "silver_path":   f"s3a://{SILVER_BUCKET}/{silver_prefix}",
            "partition_count": len(partition_folders),
            "validation":    "passed",
        }


    # ===========================================================
    # LAYER 3: AGGREGATION (Gold) — runs in parallel
    # ===========================================================

    def _run_spark_gold(table_name: str, build_fn_name: str) -> dict:
        """
        Helper: Submit a single Gold aggregation as a Spark job.

        Factored out to avoid repeating subprocess boilerplate
        in each of the 4 Gold task functions.

        Args:
            table_name    : PostgreSQL target table.
            build_fn_name : Function name in silver_to_gold module.

        Returns:
            dict: Task result metadata.
        """
        import psycopg2

        logger.info(f"Building Gold table: {table_name}")

        cmd = [
            "spark-submit",
            "--master",   SPARK_MASTER_URL,
            "--name",     f"THOR-Gold-{table_name}",
            "--packages", (
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
                "org.postgresql:postgresql:42.7.1"
            ),
            "--conf", f"spark.hadoop.fs.s3a.endpoint=http://{MINIO_ENDPOINT}",
            "--conf", f"spark.hadoop.fs.s3a.access.key={MINIO_USER}",
            "--conf", f"spark.hadoop.fs.s3a.secret.key={MINIO_PASSWORD}",
            "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
            "--conf", "spark.sql.adaptive.enabled=true",
            # Pass target table as env so silver_to_gold.py can
            # selectively build just this one aggregation
            "/opt/airflow/src/transformation/silver_to_gold.py",
            "--table", table_name,
        ]

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)

        if result.returncode != 0:
            logger.error(f"Spark STDERR:\n{result.stderr[-3000:]}")
            raise AirflowFailException(
                f"Gold job FAILED for table: {table_name}. "
                f"Return code: {result.returncode}"
            )

        # Quick row count verification via psycopg2 (no Spark overhead)
        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, dbname=PG_DB,
            user=PG_USER, password=PG_PASSWORD,
        )
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name};")
            row_count = cur.fetchone()[0]
        conn.close()

        min_rows = GOLD_MIN_ROWS.get(table_name, 1)
        if row_count < min_rows:
            raise AirflowFailException(
                f"Gold validation FAILED: {table_name} has {row_count} rows "
                f"(minimum expected: {min_rows})."
            )

        logger.info(f"✅ {table_name} → {row_count:,} rows in PostgreSQL")
        return {"table": table_name, "row_count": row_count, "status": "success"}


    @task(task_id="build_gold_year_service", execution_timeout=timedelta(minutes=30))
    def build_gold_year_service(_silver_meta: dict) -> dict:
        return _run_spark_gold(
            "gold_missions_by_year_service",
            "build_gold_missions_by_year_service"
        )

    @task(task_id="build_gold_aircraft", execution_timeout=timedelta(minutes=30))
    def build_gold_aircraft(_silver_meta: dict) -> dict:
        return _run_spark_gold(
            "gold_top_aircraft",
            "build_gold_top_aircraft"
        )

    @task(task_id="build_gold_intensity", execution_timeout=timedelta(minutes=30))
    def build_gold_intensity(_silver_meta: dict) -> dict:
        return _run_spark_gold(
            "gold_bombing_intensity_by_country",
            "build_gold_bombing_intensity_by_country"
        )

    @task(task_id="build_gold_monthly_trend", execution_timeout=timedelta(minutes=30))
    def build_gold_monthly_trend(_silver_meta: dict) -> dict:
        return _run_spark_gold(
            "gold_monthly_ops_trend",
            "build_gold_monthly_ops_trend"
        )


    # ===========================================================
    # GOLD VALIDATION (fan-in after parallel Gold tasks)
    # ===========================================================

    @task(
        task_id="validate_gold_tables",
        trigger_rule=TriggerRule.ALL_SUCCESS,  # Only if ALL 4 Gold tasks succeed
    )
    def validate_gold_tables(gold_results: list[dict]) -> dict:
        """
        Final validation: confirm all 4 Gold tables are populated.

        trigger_rule=ALL_SUCCESS: This task only runs if every
        parallel Gold task succeeded. If any failed, this task
        is skipped and the pipeline is marked failed — no silent
        partial success.
        """
        import psycopg2

        logger.info("Running final Gold layer validation...")

        conn = psycopg2.connect(
            host=PG_HOST, port=PG_PORT, dbname=PG_DB,
            user=PG_USER, password=PG_PASSWORD,
        )

        validation_report = {}
        all_passed = True

        with conn.cursor() as cur:
            for table_name, min_rows in GOLD_MIN_ROWS.items():
                cur.execute(f"SELECT COUNT(*) FROM {table_name};")
                count = cur.fetchone()[0]
                passed = count >= min_rows
                validation_report[table_name] = {
                    "row_count": count,
                    "min_expected": min_rows,
                    "passed": passed,
                }
                status = "✅" if passed else "❌"
                logger.info(
                    f"  {status} {table_name}: {count:,} rows "
                    f"(min: {min_rows})"
                )
                if not passed:
                    all_passed = False

        conn.close()

        if not all_passed:
            failed = [t for t, v in validation_report.items() if not v["passed"]]
            raise AirflowFailException(
                f"Gold validation FAILED for tables: {failed}. "
                "Check Spark logs for the corresponding build tasks."
            )

        logger.info("✅ All Gold tables validated successfully.")
        logger.info("=" * 55)
        logger.info("THOR Pipeline completed end-to-end successfully!")
        logger.info("=" * 55)

        return {"validation": "passed", "tables": validation_report}


    # ===========================================================
    # TASK WIRING — Define the dependency graph
    # ===========================================================

    # Health checks
    minio_ok    = check_minio_health()
    postgres_ok = check_postgres_health()

    # Bronze layer
    bronze_meta  = ingest_raw_to_bronze()
    bronze_valid = validate_bronze_data(bronze_meta)

    # Silver layer
    silver_meta  = transform_bronze_to_silver(bronze_valid)
    silver_valid = validate_silver_data(silver_meta)

    # Gold layer — 4 tasks in PARALLEL
    gold_year    = build_gold_year_service(silver_valid)
    gold_aircraft = build_gold_aircraft(silver_valid)
    gold_intensity = build_gold_intensity(silver_valid)
    gold_trend   = build_gold_monthly_trend(silver_valid)

    # Fan-in: all Gold tasks → final validation → end
    gold_valid = validate_gold_tables([gold_year, gold_aircraft,
                                       gold_intensity, gold_trend])

    # Wire the full graph
    start >> [minio_ok, postgres_ok] >> bronze_meta
    gold_valid >> end


# Instantiate the DAG
dag_instance = thor_medallion_pipeline()