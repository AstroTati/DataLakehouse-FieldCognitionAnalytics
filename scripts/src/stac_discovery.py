"""
STAC metadata indexing from CDSE to Delta table, source of ingestion.py.
It does not download images, it creates a catalogue with the existing scenes 
"""

import json
from datetime import datetime

from pystac_client import Client
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, ArrayType, MapType,
)

ASSET_SCHEMA = StructType([
    StructField("href", StringType(), True),
    StructField("type", StringType(), True),
    StructField("title", StringType(), True),
    StructField("roles", ArrayType(StringType()), True),
])

STAC_SCHEMA = StructType([
    StructField("id", StringType(), False),
    StructField("type", StringType(), True),
    StructField("stac_version", StringType(), True),
    StructField("collection", StringType(), True),
    StructField("bbox", ArrayType(DoubleType()), True),
    StructField("geometry", StringType(), True),  # serealize GeoJSON as string
    StructField("properties", MapType(StringType(), StringType()), True),
    StructField("assets", MapType(StringType(), ASSET_SCHEMA), True),
])


def _process_stac_items(raw_items_dict: list) -> list:
    # Serialize geometry/properties (dynamic dicts) so Spark can parse them reliably
    processed = []
    for d in raw_items_dict:
        processed.append({
            "id": d.get("id"),
            "type": d.get("type"),
            "stac_version": d.get("stac_version"),
            "collection": d.get("collection"),
            "bbox": [float(b) for b in d.get("bbox", [])],
            "geometry": json.dumps(d.get("geometry")),
            "properties": {k: str(v) for k, v in d.get("properties", {}).items()},
            "assets": {
                k: {
                    "href": v.get("href"),
                    "type": v.get("type"),
                    "title": v.get("title"),
                    "roles": v.get("roles"),
                } for k, v in d.get("assets", {}).items()
            },
        })
    return processed


def discover_scenes(catalog_url: str, collection: str, bbox: list,
                     start_date: str, end_date: str, limit: int = 100) -> list:
    #Checks on the CDSE STAC API and returns a list of raw items (dicts)
    datetime_range = f"{start_date}T00:00:00Z/{end_date}T23:59:59Z"

    print(f"Connecting to Copernicus STAC API at {catalog_url}...")
    catalog = Client.open(catalog_url)

    search = catalog.search(
        collections=[collection],
        bbox=bbox,
        datetime=datetime_range,
        limit=limit,
    )

    stac_items = list(search.items())
    print(f"Retrieved {len(stac_items)} STAC items from CDSE.")

    if not stac_items:
        raise ValueError("No STAC items found matching the given criteria.")

    return [item.to_dict() for item in stac_items]


def index_scenes_to_delta(spark, raw_items_dict: list, target_table: str,
                           catalog_url: str, aoi_id: str) -> int:
    """Transforms raw STAC items in Bronze rows and appends them to the 
    indexed Delta table. 
    Uses append + anti-join (no overwrite) to make sure multiple AOIs can
    coexist in the same table without stepping on each other in different runs.
    """
    processed_records = _process_stac_items(raw_items_dict)
    df_raw = spark.createDataFrame(processed_records, schema=STAC_SCHEMA)

    df_bronze = (
        df_raw
        .withColumn("acquisition_time", F.to_timestamp(F.col("properties")["datetime"]))
        .withColumn("cloud_cover", F.col("properties")["eo:cloud_cover"].cast("double"))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("source_catalog", F.lit(catalog_url))
        .withColumn("aoi_id", F.lit(aoi_id))
    )

    # Anti-join: don't re-append already indexed scenes for this AOI (dedup)
    if spark.catalog.tableExists(target_table):
        already_indexed = (
            spark.table(target_table)
            .filter(f"aoi_id = '{aoi_id}'")
            .select("id")
        )
        df_bronze = df_bronze.join(already_indexed, on="id", how="left_anti")

    new_count = df_bronze.count()
    if new_count == 0:
        print(f"No new scenes to index (aoi_id={aoi_id}).")
        return 0

    (
        df_bronze.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .partitionBy("collection")
        .saveAsTable(target_table)
    )

    print(f"Indexed {new_count} new scenes in {target_table} (aoi_id={aoi_id}).")
    return new_count