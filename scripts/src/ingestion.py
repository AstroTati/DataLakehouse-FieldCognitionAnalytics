'''
Resolves OData product
Downloads .zim with checksum verification
Writes enriched metadata 
Logs ingestion result 
'''

import os
import time
import hashlib
import json
from datetime import datetime
from typing import Callable
import requests
from tqdm import tqdm
from src.exceptions import PermanentIngestionError, TransientIngestionError
from src.cdse_client import resolve_odata_product_id
from src.tile_utils import extract_mgrs_tile
from src.metadata import build_enriched_metadata
from src.control_table import flush_log_records, get_run_id, make_log_record

TOKEN_REFRESH_SECONDS = 540  # CDSE tokens expire after 10 min -> refresh at 9 min


def _extract_expected_md5(odata_product: dict) -> str | None:
    #CDSE OData 'Checksum' is a list like [{'Algorithm': 'MD5', 'Value': '...'}]
    for entry in odata_product.get("Checksum", []) or []:
        if entry.get("Algorithm", "").upper() == "MD5":
            return entry.get("Value", "").lower()
    return None


def _cleanup_partial_file(path: str):
    # Deletes partial and corrupt .zip files
    try:
        if os.path.exists(path):
            os.remove(path)
            print(f"  -> cleaned up partial file: {path}")
    except OSError as e:
        print(f"  -> WARNING: could not remove partial file {path}: {e}")


def download_safe_zip(odata_product_id: str, token: str, dest_path: str, odata_base: str) -> tuple[int, str]:
    # Downloads the .zip file in chunks of 8MB, calculates MD5 in parallel.
    # Returns (bytes_written, md5_hexdigest)
    url = f"{odata_base}/Products({odata_product_id})/$value"
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}"})

    try:
        response = session.get(url, stream=True, allow_redirects=False, timeout=60)
        if response.status_code in (301, 302, 303, 307):
            response = session.get(response.headers["Location"], stream=True, timeout=60)

        if response.status_code >= 500:
            raise TransientIngestionError(f"Download returned {response.status_code}")
        response.raise_for_status()

        bytes_written = 0
        md5_hash = hashlib.md5()
        with open(dest_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
                if chunk:
                    f.write(chunk)
                    md5_hash.update(chunk)
                    bytes_written += len(chunk)

        return bytes_written, md5_hash.hexdigest()

    except requests.exceptions.RequestException as e:
        _cleanup_partial_file(dest_path)
        raise TransientIngestionError(f"Download network error: {e}") from e


def verify_download(bytes_written: int, actual_md5: str, odata_product: dict, dest_path: str):
    # Launchges PermanentIngestionError (and deletes it) if size or checksum don't match what OData reported
    expected_size = odata_product.get("ContentLength")
    if expected_size is not None and bytes_written != expected_size:
        _cleanup_partial_file(dest_path)
        raise PermanentIngestionError(
            f"Size mismatch: expected {expected_size} bytes, got {bytes_written}"
        )

    expected_md5 = _extract_expected_md5(odata_product)
    if expected_md5 is not None and actual_md5 != expected_md5:
        _cleanup_partial_file(dest_path)
        raise PermanentIngestionError(
            f"Checksum mismatch: expected MD5 {expected_md5}, got {actual_md5}"
        )

# ----------------------------------------------------------------------------------------
def run_ingestion(
    spark,
    config: dict,
    get_token: Callable[[], str],
    already_ingested,
    control_table: str,
    max_cloud_cover: float,
) -> None:
    uc = config["unity_catalog"]
    cdse = config["cdse"]
    retry = config["retry_policy"]
    ingestion_cfg = config["ingestion"]
    active_aoi = config["stac_discovery"]["active_aoi"] 

    volume_root = f"/Volumes/{uc['catalog']}/{uc['bronze_schema']}/{uc['volume_name']}"
    odata_base = cdse["odata_base"]
    max_retries = retry["max_retries"]
    backoff_base = retry["backoff_base_seconds"]
    log_flush_every = ingestion_cfg["log_flush_every"]

    run_id = get_run_id(spark)

    scenes_df = (
        spark.table(uc["stac_source_table"])
        .filter(f"aoi_id = '{active_aoi}'")  
        .filter(f"cloud_cover <= {max_cloud_cover}")
        .join(already_ingested, on="id", how="left_anti")
    )

    scenes = [row.asDict() for row in scenes_df.collect()]
    print(f"{len(scenes)} scenes pending ingestion.")

    token = get_token()
    token_issued_at = time.time()
    log_records = []

    for i, scene in enumerate(tqdm(scenes, desc="Ingesting scenes", unit="scene")):
        if time.time() - token_issued_at > TOKEN_REFRESH_SECONDS:
            token = get_token()
            token_issued_at = time.time()

        raw_id = scene["id"]
        product_name = raw_id.replace(".SAFE", "")
        started_at = datetime.utcnow()
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]: [{i+1}/{len(scenes)}] Processing {product_name}...")

        tile_id = None
        acquisition_date = None
        product_dir = None
        attempt = 0

        while True:
            attempt += 1
            try:
                odata_product = resolve_odata_product_id(product_name, token, odata_base)
                tile_id = extract_mgrs_tile(product_name)

                acq_time = scene["acquisition_time"]
                if isinstance(acq_time, str):
                    acq_time = datetime.fromisoformat(acq_time.replace("Z", "+00:00"))
                acquisition_date = acq_time.strftime("%Y_%m_%d")

                product_dir = f"{volume_root}/{tile_id}/{acquisition_date}/{product_name}"
                os.makedirs(product_dir, exist_ok=True)
                zip_dest = f"{product_dir}/{product_name}.zip"
                meta_dest = f"{product_dir}/metadata.json"

                bytes_written, actual_md5 = download_safe_zip(odata_product["Id"], token, zip_dest, odata_base)
                verify_download(bytes_written, actual_md5, odata_product, zip_dest)

                metadata = build_enriched_metadata(
                    scene, odata_product, bytes_written, actual_md5,
                    odata_base_url=odata_base,
                )
                with open(meta_dest, "w") as f:
                    json.dump(metadata, f, indent=2, default=str)

                print(f"  -> OK: written in {product_dir}/ ({bytes_written} bytes, md5 verified)")

                log_records.append(make_log_record(
                    product_name, odata_product.get("Checksum"), tile_id, acquisition_date,
                    "success", None, bytes_written, product_dir, started_at, run_id, active_aoi
                ))
                break

            except PermanentIngestionError as e:
                print(f"  -> PERMANENT FAILURE in {product_name}: {e}")
                log_records.append(make_log_record(
                    product_name, None, tile_id, acquisition_date,
                    "failed_permanent", str(e)[:500], None, product_dir, started_at, run_id, active_aoi
                ))
                break

            except TransientIngestionError as e:
                if attempt >= max_retries:
                    print(f"  -> TRANSIENT FAILURE in {product_name} (gave up after {attempt} attempts): {e}")
                    log_records.append(make_log_record(
                        product_name, None, tile_id, acquisition_date,
                        "failed_retryable", str(e)[:500], None, product_dir, started_at, run_id, active_aoi
                    ))
                    break
                backoff = backoff_base * (2 ** (attempt - 1))
                print(f"  -> transient error (attempt {attempt}/{max_retries}), retrying in {backoff}s: {e}")
                time.sleep(backoff)
                continue

            except Exception as e:
                print(f"  -> UNEXPECTED ERROR in {product_name}: {e}")
                log_records.append(make_log_record(
                    product_name, None, tile_id, acquisition_date,
                    "failed_permanent", f"unexpected: {str(e)[:480]}", None, product_dir, started_at, run_id, active_aoi
                ))
                break

        if len(log_records) >= log_flush_every:
            flush_log_records(spark, log_records, control_table)

    flush_log_records(spark, log_records, control_table)