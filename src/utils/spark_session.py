# =============================================================
# src/utils/spark_session.py
#
# Purpose : Centralised SparkSession factory.
#
# Key Design Decisions:
#   1. S3A Connector  : Spark reads/writes MinIO via the Hadoop
#                       S3A filesystem — identical API to AWS S3.
#                       This makes the pipeline cloud-portable:
#                       swap MinIO endpoint → runs on AWS S3.
#   2. Parquet Optimisations:
#      - columnar reads (predicate pushdown)
#      - snappy compression (fast + splittable)
#   3. Adaptive Query Execution (AQE): Spark 3.x feature that
#      dynamically re-optimises plans at runtime — handles
#      data skew automatically at join & shuffle stages.
#   4. Kryo Serialization: ~10x faster than Java default for
#      shuffles across worker nodes.
# =============================================================

import os
import logging
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def get_spark_session(
    app_name: str = "THOR-Pipeline",
    master: str | None = None,
    enable_hive: bool = False,
) -> SparkSession:
    """
    Build and return a configured SparkSession.

    The session is configured for:
      - MinIO access via S3A (Hadoop AWS connector)
      - Parquet read/write with Snappy compression
      - Adaptive Query Execution (AQE) enabled
      - Kryo serialization for shuffle performance

    Args:
        app_name    : Spark UI display name.
        master      : Spark master URL. Defaults to env var
                      SPARK_MASTER_URL or "local[*]" for dev.
        enable_hive : Enable Hive metastore support (not needed
                      for this project but left as an option).

    Returns:
        SparkSession: Ready-to-use Spark session.
    """
    master_url = master or os.getenv("SPARK_MASTER_URL", "local[*]")

    minio_endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000") \
                       .replace("http://", "")
    minio_user     = os.getenv("MINIO_ROOT_USER")
    minio_password = os.getenv("MINIO_ROOT_PASSWORD")

    logger.info(f"Initialising SparkSession | app: {app_name} | master: {master_url}")

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master_url)

        # ── S3A / MinIO Configuration ──────────────────────────
        # fs.s3a.endpoint       : Point S3A at MinIO instead of AWS
        # fs.s3a.path.style.access: MinIO requires path-style URLs
        #   (s3a://bucket/key) NOT virtual-hosted (bucket.s3.amazonaws.com)
        # fs.s3a.connection.ssl.enabled: Off for local HTTP
        .config("spark.hadoop.fs.s3a.endpoint",               f"http://{minio_endpoint}")
        .config("spark.hadoop.fs.s3a.access.key",             minio_user)
        .config("spark.hadoop.fs.s3a.secret.key",             minio_password)
        .config("spark.hadoop.fs.s3a.path.style.access",      "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        # ── Vá lỗi NumberFormatException "60s" và "24h" của Hadoop 3.3.4 ──
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
        .config("spark.hadoop.fs.s3a.connection.timeout", "600000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "600000")
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400") 
        
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

        # ── I/O Optimisations ──────────────────────────────────
        # Parquet: enable predicate pushdown & dictionary encoding
        .config("spark.sql.parquet.filterPushdown",     "true")
        .config("spark.sql.parquet.writeLegacyFormat",  "false")
        .config("spark.sql.parquet.compression.codec",  "snappy")

        # ── Adaptive Query Execution (AQE) ─────────────────────
        # AQE dynamically:
        #   - Coalesces shuffle partitions (avoids tiny files)
        #   - Converts SortMergeJoin → BroadcastJoin at runtime
        #   - Splits skewed partitions automatically
        # We ALSO apply manual salting (Phase 3.3) to demonstrate
        # deep understanding BEYOND just enabling AQE.
        .config("spark.sql.adaptive.enabled",                     "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled",  "true")
        .config("spark.sql.adaptive.skewJoin.enabled",            "true")
        .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
        .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")

        # ── Serialization ──────────────────────────────────────
        # Kryo is significantly faster than Java serialization
        # for network shuffles between executors
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryo.unsafe", "true")

        # ── Time Parser Policy ─────────────────────────────────
        # Ép Spark sử dụng trình phân tích ngày tháng cũ (Legacy),
        # giúp to_date trả về Null khi sai định dạng thay vì làm sập chương trình.
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")


        # ── Shuffle & Memory ───────────────────────────────────
        # 200 is the Spark default; with 4.5M rows and good
        # partitioning we keep it. AQE will coalesce at runtime.
        .config("spark.sql.shuffle.partitions", "200")

        # Broadcast join threshold: DataFrames < 20MB are
        # automatically broadcast. We set this explicitly so the
        # value appears in Spark UI (useful during demo/review).
        .config("spark.sql.autoBroadcastJoinThreshold", str(20 * 1024 * 1024))

        # ── JARs for S3A (must be on classpath) ────────────────
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "org.postgresql:postgresql:42.6.0"
        )
    )

    if enable_hive:
        builder = builder.enableHiveSupport()

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")  # Suppress INFO noise

    logger.info(f"SparkSession ready | version: {spark.version}")
    return spark