# MGRS extraction (from tile_id) from the Sentinel-2 product name

from src.exceptions import PermanentIngestionError


def extract_mgrs_tile(product_name: str) -> str:
    clean_name = product_name.replace(".SAFE", "")
    parts = clean_name.split("_")
    tile_part = next((p for p in parts if p.startswith("T") and len(p) == 6), None)

    if tile_part is None:
        raise PermanentIngestionError(f"Unable to extract tile_id from: {product_name}")
    return tile_part