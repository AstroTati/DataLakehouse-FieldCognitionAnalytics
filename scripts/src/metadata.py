
# This script extracts the tile_id and builds the enriched metadata.jason for a downloaded and verifies scene. 
from datetime import datetime

def extract_mgrs_tile(product_name: str) -> str:
    clean_name = product_name.replace(".SAFE", "")
    parts = clean_name.split("_")
    tile_part = next((p for p in parts if p.startswith("T") and len(p) == 6), None)

    if tile_part is None:
        raise PermanentIngestionError(f"Unable to extract tile_id from: {product_name}")
    return tile_part


def build_enriched_metadata(
    stac_row: dict,
    odata_product: dict,
    bytes_written: int,
    actual_md5: str,
    odata_base_url: str,
) -> dict:
    return {
        "product_name": stac_row["id"],
        "collection": stac_row["collection"],
        "tile_id": extract_mgrs_tile(stac_row["id"]),
        "acquisition_time": stac_row["acquisition_time"],
        # "cloud_cover_pct": stac_row["cloud_cover"],
        "bbox": stac_row["bbox"],
        "odata_product_id": odata_product["Id"],
        "content_length_bytes": odata_product.get("ContentLength"),
        "checksum": odata_product.get("Checksum"),
        "downloaded_bytes": bytes_written,
        "downloaded_md5": actual_md5,
        "source_catalog_stac": "https://stac.dataspace.copernicus.eu/v1",
        "source_catalog_odata": odata_base_url,
        "storage_backend": "unity_catalog_volume", 
        "ingested_at": datetime.utcnow().isoformat() + "Z",
    }
