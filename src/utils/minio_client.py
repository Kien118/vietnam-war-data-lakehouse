# =============================================================
# src/utils/minio_client.py
#
# Purpose : Reusable MinIO (S3-compatible) client factory.
#           Centralises all connection logic so every other
#           module only needs to call get_minio_client().
# Pattern : Factory function — keeps credentials out of
#           business logic (12-factor app principle).
# =============================================================

import os
import logging
from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)


def get_minio_client() -> Minio:
    """
    Create and return a MinIO client using environment variables.

    Environment Variables Required:
        MINIO_ENDPOINT      : e.g. "localhost:9000"
        MINIO_ROOT_USER     : Access key
        MINIO_ROOT_PASSWORD : Secret key
        MINIO_SECURE        : "true"/"false" (default: false for local)

    Returns:
        Minio: Configured MinIO client instance.

    Raises:
        EnvironmentError: If required env vars are missing.
    """
    endpoint = os.getenv("MINIO_ENDPOINT", "host.docker.internal:9005")
    
    # Cung cấp mặc định cứng nếu Airflow không tìm thấy biến môi trường
    access_key = os.getenv("MINIO_ROOT_USER", "minio_admin")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minio_secure_pass_2025")
    secure = os.getenv("MINIO_SECURE", "false").lower() == "true"


    # Strip the http:// or https:// prefix if accidentally included
    # MinIO client expects just "host:port"
    endpoint = endpoint.replace("http://", "").replace("https://", "")

    client = Minio(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
    )

    logger.info(f"MinIO client initialized → endpoint: {endpoint}, secure: {secure}")
    return client


def ensure_bucket_exists(client: Minio, bucket_name: str) -> None:
    """
    Create a bucket if it does not already exist.

    Args:
        client      : Active MinIO client.
        bucket_name : Name of the bucket to ensure.
    """
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logger.info(f"Bucket created: '{bucket_name}'")
        else:
            logger.info(f"Bucket already exists: '{bucket_name}'")
    except S3Error as e:
        logger.error(f"Failed to ensure bucket '{bucket_name}': {e}")
        raise


def list_objects_in_bucket(client: Minio, bucket_name: str, prefix: str = "") -> list:
    """
    List all object names in a bucket (optionally filtered by prefix).

    Args:
        client      : Active MinIO client.
        bucket_name : Target bucket.
        prefix      : Optional key prefix filter (e.g. "vietnam/2024/").

    Returns:
        list[str]: List of object names (keys).
    """
    try:
        objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)
        keys = [obj.object_name for obj in objects]
        logger.info(f"Found {len(keys)} objects in '{bucket_name}/{prefix}'")
        return keys
    except S3Error as e:
        logger.error(f"Failed to list objects in '{bucket_name}': {e}")
        raise