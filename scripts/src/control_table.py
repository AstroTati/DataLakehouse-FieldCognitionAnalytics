# Control table script (idempotency, traceability, batched logging)

import json
from datetime import datetime

from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, TimestampType,
)


def ensure_control_schema_exists(spark, catalog: str, control_schema: str):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{control_schema}")

def get_already_ingested(spark, control_table: str):
    if not spark.catalog.tableExists(control_table):
        return spark.createDataFrame([], "id string")

    return (
        spark.table(control_table)
        .filter("status = 'success'")
        .select("product_name")
        .distinct()
        .withColumnRenamed("product_name", "id")
    )


def flush_log_records(spark, log_records: list, control_table: str):
    """Batched append into the control table. It is called every 
    LOG_FLUSH_EVERY scenes and once more at the end instead of writing
    per scene to avoid the small file problem in Delta."""
    if not log_records:
        return

    schema = StructType([
        StructField("product_name", StringType(), True),
        StructField("checksum", StringType(), True),
        StructField("tile_id", StringType(), True),
        StructField("acquisition_date", StringType(), True),
        StructField("status", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("file_size_bytes", LongType(), True),
        StructField("storage_path", StringType(), True),
        StructField("ingestion_started_at", TimestampType(), True),
        StructField("ingestion_completed_at", TimestampType(), True),
        StructField("aoi_id", StringType(), True),
        StructField("databricks_run_id", StringType(), True),
    ])

    for record in log_records:
        if record.get("checksum") is not None:
            record["checksum"] = json.dumps(record["checksum"])

    log_df = spark.createDataFrame(log_records, schema=schema)
    (log_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(control_table))
    print(f"  [log] flushed {len(log_records)} record(s) to {control_table}")
    log_records.clear()


def get_run_id(spark) -> str:
    """spark.conf.get doesn't work on default: Databricks returns
    CONFIG_NOT_AVAILABLE in interactive runs instead of the default value."""
    try:
        return spark.conf.get("spark.databricks.job.runId")
    except Exception:
        return "interactive"


def make_log_record(product_name, checksum, tile_id, acquisition_date, status,
                     error_message, file_size_bytes, storage_path,
                     started_at, run_id, aoi_id):
    return {
        "product_name": product_name,
        "checksum": checksum,
        "tile_id": tile_id,
        "acquisition_date": acquisition_date,
        "status": status,
        "error_message": error_message,
        "file_size_bytes": file_size_bytes,
        "storage_path": storage_path,
        "ingestion_started_at": started_at,
        "ingestion_completed_at": datetime.utcnow(),
        "aoi_id": aoi_id,  
        "databricks_run_id": run_id,
    }