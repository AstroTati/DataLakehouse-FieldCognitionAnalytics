"""
================================================================================
BRONZE LAYER INGESTION — Sentinel-2 STAC Metadata
================================================================================
WHAT THIS SCRIPT DOES (one sentence):
  It asks the Copernicus satellite catalog "what Sentinel-2 images exist for
  this map area and this date range?", takes the answer (metadata only, no
  actual images), and saves it as a structured table in our data lake.

WHY THIS MATTERS (Medallion architecture, for those new to the term):
  We organize data in three quality tiers:
    BRONZE -> raw data, minimally touched, kept close to its original form
    SILVER -> cleaned, filtered, joined, ready for analysis
    GOLD   -> aggregated, business-ready, dashboard/model-ready

  This script produces the BRONZE table. Nothing here is "cleaned" yet on
  purpose — the goal of Bronze is to have a reliable, replayable copy of what
  the source system gave us, so if something breaks downstream, we can
  reprocess from Bronze instead of re-querying the external API again.
================================================================================
"""

import json
from datetime import datetime
from pystac_client import Client
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, ArrayType, TimestampType, MapType
)

# Spark is the distributed processing engine we use instead of pandas once
# data volumes get large. Think of a Spark DataFrame like a pandas DataFrame,
# but one that can be split across many machines. For this script's data
# volume (a few hundred rows of metadata) that scale isn't strictly needed —
# but writing to Delta Lake / Unity Catalog (our governed table storage)
# requires going through Spark.
spark = SparkSession.builder.getOrCreate()

# -------------------------------------------------------------------------
# 1. PARAMETERS & INPUTS
# -------------------------------------------------------------------------
# These are the "knobs" of the job. In a production setup these would come
# from a config file or workflow parameters (e.g. Databricks Job parameters)
# instead of being hardcoded, so the same script can be reused for different
# regions/time windows without editing code.

CATALOG_URL = "https://stac.dataspace.copernicus.eu/v1"
# STAC = SpatioTemporal Asset Catalog. It's an open standard (not
# Copernicus-specific) for describing "what satellite scenes exist and
# where/when were they taken", without having to download the imagery
# itself first. Think of it like a searchable index/card catalog for
# satellite data.

COLLECTION = "sentinel-2-l2a"
# A "collection" in STAC terms = one imagery product line.
# L2A = Level-2A = atmospherically corrected surface reflectance
# (as opposed to L1C, which is closer to raw sensor data). L2A is what
# most analysis-ready workflows use.

TARGET_DELTA_TABLE = "fca_copernicus.bronze.sentinel2_stac_raw"
# This is a three-part Unity Catalog table name: <catalog>.<schema>.<table>
# Equivalent mental model to a namespaced database.table in SQL.
# "bronze" here is literally the schema/folder name we use to mark this
# table as raw-tier data, consistent with the Medallion naming convention.

# Define Area of Interest: [min_lon, min_lat, max_lon, max_lat]
BBOX = [11.0, 47.0, 11.5, 47.5]
# A simple rectangular bounding box in WGS84 lon/lat degrees.
# This one covers roughly a 40km x 55km patch in the Alps (northern
# Italy/Austria border area). Later pipeline stages could swap this for
# a more precise polygon (AOI) if we need non-rectangular regions.

# Define Temporal Window
START_DATE = "2024-06-01"
END_DATE = "2024-06-30"
DATETIME_RANGE = f"{START_DATE}T00:00:00Z/{END_DATE}T23:59:59Z"
# STAC search APIs expect an ISO 8601 datetime *range* string, using "/" to
# separate start and end. This is a query-string formatting requirement of
# the STAC spec, not something specific to Copernicus.

# -------------------------------------------------------------------------
# 2. QUERY COPERNICUS STAC API
# -------------------------------------------------------------------------
print(f">>> [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]: BEGIN DATA INGESTION")
print(f"Connecting to Copernicus STAC API at {CATALOG_URL}...")
catalog = Client.open(CATALOG_URL)
# pystac_client is a Python SDK that speaks the STAC API protocol for us —
# handles pagination, request formatting, etc., so we don't hand-roll HTTP
# calls.

search = catalog.search(
    collections=[COLLECTION],
    bbox=BBOX,
    datetime=DATETIME_RANGE,
    limit=100
)
# This sends the actual search request: "give me Sentinel-2 L2A scenes
# that intersect this bounding box, within this date range."
# limit=100 caps the page size the API returns per request (pystac_client
# transparently pages through more if there are additional results when we
# call .items() below).

stac_items = list(search.items())
# IMPORTANT: at this point we only have *metadata* — scene ID, footprint,
# cloud cover, acquisition date, and *links* (URLs) to the actual band
# files. No image pixels have been downloaded yet. That download only
# happens later, in the Silver-layer stage that feeds the UNet model.
print(f"Retrieved {len(stac_items)} STAC items from CDSE.")

if not stac_items:
    raise ValueError("No STAC items found matching the given criteria.")
# A deliberate "fail loud" choice: if the search comes back empty, we stop
# the job instead of silently writing an empty table. In a scheduled
# pipeline, a silent empty write is worse than a visible failure — it can
# hide real problems (e.g. wrong AOI, expired API, changed collection name)
# from anyone monitoring the job.

# Convert PySTAC Item objects into raw PyDicts
raw_items_dict = [item.to_dict() for item in stac_items]
# pystac_client gives us back Python objects (STAC "Item" class instances),
# not plain dictionaries. Spark has no idea what a pystac Item object is,
# so we convert each one to a plain dict — the universal, framework-agnostic
# data shape everything downstream can work with.

# -------------------------------------------------------------------------
# 3. SPARK SCHEMA DEFINITION
# -------------------------------------------------------------------------
# ---- WHY THIS STEP EXISTS AT ALL (the core DE concept in this script) ----
#
# In pandas, you can just do pd.DataFrame(list_of_dicts) and pandas will
# figure out the columns and types for you automatically, on the fly.
#
# Spark *can* do the same thing (it's called "schema inference") — but for
# a job that will run unattended, on a schedule, potentially over
# irregular/nested data, schema inference is risky for two concrete
# reasons we care about here:
#
#   1. PERFORMANCE: to infer a schema, Spark has to first scan through all
#      the data once just to guess the types, and THEN scan through it
#      again to actually load it. On small data (our case, ~100 rows)
#      that's invisible. At production scale, it's a real, avoidable cost
#      we pay on every single run.
#
#   2. RELIABILITY / TYPE SAFETY: STAC metadata is what's called
#      "semi-structured" — the fields for one satellite scene can differ
#      slightly from another (e.g. optional properties present on some
#      scenes but not others, or a value that's a number for scene A but
#      a string for scene B due to a source inconsistency). If we let
#      Spark guess the schema, a single unexpected value can silently
#      change the inferred column type between runs, or crash the job
#      partway through a write. By writing the schema explicitly, we are
#      telling Spark "these are the exact columns and types I expect,
#      always" — any mismatch fails immediately and visibly, at the start,
#      rather than causing subtle bugs downstream in Silver/Gold tables
#      that consume this Bronze table.
#
# This is a very common Data Engineering pattern: trade a bit of upfront
# verbosity for predictability and speed at scale. Data science work
# tends to favor the opposite trade-off (fast, flexible, inferred) because
# notebooks are usually run interactively by a human who can react to
# a surprise. A scheduled pipeline has no human watching it in real time,
# so we remove the surprises in advance.

# ---- asset_schema: describes ONE downloadable file tied to a scene ----
# A single Sentinel-2 "item" (scene) doesn't have just one file — it has
# many: one per spectral band (B02=blue, B03=green, B04=red, B08=NIR...),
# plus a thumbnail, a cloud mask, etc. In STAC these are called "assets".
# This schema describes the shape of ONE of those asset entries.
asset_schema = StructType([
    StructField("href", StringType(), True),   # the actual download URL for this file
    StructField("type", StringType(), True),   # file format, e.g. "image/tiff; application=geotiff"
    StructField("title", StringType(), True),  # human-readable name, e.g. "Red - 10m"
    StructField("roles", ArrayType(StringType()), True)
    # "roles" is a list because one asset can serve multiple purposes at
    # once, e.g. both "data" and "reflectance" — ArrayType is Spark's
    # equivalent of a Python list column.
])
# StructType/StructField is Spark's way of defining "this is a nested
# object with named, typed fields" — conceptually identical to defining a
# dataclass or a JSON schema, just in Spark's own syntax.

# ---- stac_schema: describes ONE full satellite scene (one row of our table) ----
stac_schema = StructType([
    StructField("id", StringType(), False),
    # False here means "not nullable" — every scene MUST have an ID, or
    # the row is invalid. This is us encoding a business rule directly
    # into the schema.
    StructField("type", StringType(), True),
    StructField("stac_version", StringType(), True),
    StructField("collection", StringType(), True),
    StructField("bbox", ArrayType(DoubleType()), True),
    # bbox = [min_lon, min_lat, max_lon, max_lat] for this specific scene's
    # footprint — a list of 4 decimal numbers, hence ArrayType(DoubleType()).

    StructField("geometry", StringType(), True),
    # The scene footprint is usually a polygon (GeoJSON), which is a
    # nested, variable-shaped structure (could be a simple rectangle or a
    # complex multi-part shape for scenes crossing tile boundaries).
    # Rather than modeling that variability in Spark's type system, we
    # store it as a plain JSON string and parse it later, only if/when a
    # downstream step needs actual geometry operations. This is a common,
    # pragmatic DE trade-off: don't over-engineer the schema for a field
    # you're not filtering/joining on yet.

    StructField("properties", MapType(StringType(), StringType()), True),
    # MapType = Spark's equivalent of a Python dict column, i.e.
    # key-value pairs. STAC "properties" is a loose bag of ~15-30
    # attributes (cloud cover, acquisition datetime, sun angle, orbit
    # number, processing baseline, etc.) that isn't fully consistent
    # across every provider/collection. Rather than hardcode 30 separate
    # typed columns (fragile — breaks the moment Copernicus adds/renames
    # one), we keep it as a flexible key->value map of strings and pull
    # out only the specific fields we actually need as real typed columns
    # below (acquisition_time, cloud_cover). This is another common DE
    # pattern: keep the long tail of "maybe useful someday" fields
    # available but unopinionated, and only "promote" the fields you know
    # you need into first-class, properly typed columns.

    StructField("assets", MapType(StringType(), asset_schema), True)
    # This nests our asset_schema from above: "assets" is a dictionary
    # where the key is the band/file name (e.g. "B04", "thumbnail") and
    # the value is the asset_schema struct describing that file. This is
    # the Spark equivalent of: Dict[str, AssetObject].
])

# ---- Manual pre-processing before handing data to Spark ----
# Even with an explicit schema, we still need to reshape the raw dicts
# slightly so their *values* match what we declared above — Spark's schema
# declares the target shape, it doesn't auto-convert mismatched input.
processed_records = []
for d in raw_items_dict:
    record = {
        "id": d.get("id"),
        "type": d.get("type"),
        "stac_version": d.get("stac_version"),
        "collection": d.get("collection"),
        "bbox": [float(b) for b in d.get("bbox", [])],
        # force every bbox value to a Python float, matching DoubleType()

        "geometry": json.dumps(d.get("geometry")),
        # serialize the nested GeoJSON dict down to a plain string, to
        # match the StringType() we declared for "geometry" above

        "properties": {k: str(v) for k, v in d.get("properties", {}).items()},
        # force every properties value to a string, matching
        # MapType(StringType(), StringType()) — this is why we cast
        # cloud_cover back to double explicitly further down: something
        # has to "undo" this stringification for the one or two fields we
        # actually need as numbers.

        "assets": {
            k: {
                "href": v.get("href"),
                "type": v.get("type"),
                "title": v.get("title"),
                "roles": v.get("roles")
            } for k, v in d.get("assets", {}).items()
        }
        # rebuild each asset dict so it contains *exactly* the four fields
        # declared in asset_schema, in the right shape — any extra fields
        # STAC might return that we didn't declare get dropped here.
    }
    processed_records.append(record)

# -------------------------------------------------------------------------
# 4. CREATE SPARK DATAFRAME & TRANSFORM TO BRONZE METADATA
# -------------------------------------------------------------------------
df_raw = spark.createDataFrame(processed_records, schema=stac_schema)
# This is the moment the data actually becomes a Spark DataFrame, using the
# schema we defined above instead of letting Spark guess it.

df_bronze = (
    df_raw
    .withColumn("acquisition_time", F.to_timestamp(F.col("properties")["datetime"]))
    # Pull the "datetime" key back out of the properties map (remember,
    # everything in that map is currently a string) and parse it into a
    # real timestamp type. This is one of the "promote to first-class
    # column" moves mentioned above.

    .withColumn("cloud_cover", F.col("properties")["eo:cloud_cover"].cast("double"))
    # Same idea: pull cloud cover percentage out of the generic map and
    # cast it to a proper number. This becomes the main filter downstream
    # (Silver layer) to discard unusably cloudy scenes before they ever
    # reach the segmentation model.

    .withColumn("ingested_at", F.current_timestamp())
    # Audit column: records exactly when this row was written. Useful for
    # debugging and for data lineage.

    .withColumn("source_catalog", F.lit(CATALOG_URL))
    # Audit column: records which system/URL this row came from. Becomes
    # important once we ingest from more than one satellite data provider.
)

# -------------------------------------------------------------------------
# 5. WRITE TO BRONZE DELTA TABLE IN UNITY CATALOG (FULL OVERWRITE)
# -------------------------------------------------------------------------
(
    df_bronze
    .write
    .format("delta")
    # Delta Lake = a storage format (built on Parquet) that adds
    # versioning, transactional writes (no half-finished/corrupt writes),
    # and time-travel (query the table as it looked yesterday). It's the
    # default storage format in Databricks/Unity Catalog.

    .mode("overwrite")
    # Replaces the ENTIRE table contents with this run's results every
    # time the script runs. Fine for development/prototyping where we
    # want a clean, reproducible table each run. NOTE: in production
    # we'd typically switch to mode("append") plus a dedup/merge strategy,
    # so a June run doesn't erase May's ingested rows.

    .option("overwriteSchema", "true")
    # Allows the table's column definitions to change between runs (e.g.
    # if we add a new column later). Without this flag, Spark would
    # refuse to overwrite a table whose schema doesn't exactly match what
    # already exists — a safety guard we're intentionally disabling here
    # because we're still iterating on the schema during development.

    .partitionBy("collection")
    # Physically splits the stored files by the "collection" column's
    # value, similar in spirit to folder-per-category. This speeds up
    # future queries that filter by collection. NOTE: since this script
    # currently only ever ingests ONE collection at a time
    # ("sentinel-2-l2a"), this partitioning has no real effect yet — it
    # becomes useful once we ingest multiple collections (e.g. add
    # Sentinel-1) into the same table.

    .saveAsTable(TARGET_DELTA_TABLE)
)

print(f"Successfully overwritten {TARGET_DELTA_TABLE} with {df_bronze.count()} records.")
print(f">>> [{datetime.now()}]: DATA INGESTION COMPLETED!")
