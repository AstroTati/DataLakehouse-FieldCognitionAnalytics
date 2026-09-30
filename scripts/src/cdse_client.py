# This script gets an OAuth2 access token from CDSE (Copernicus Data Space Ecosystem).

import requests
from src.exceptions import PermanentIngestionError, TransientIngestionError

def get_cdse_access_token(username: str, password: str, token_url: str) -> str:
    resp = requests.post(
        token_url,
        data={
            "client_id": "cdse-public",
            "username": username,
            "password": password,
            "grant_type": "password",
        },
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def resolve_odata_product_id(product_name: str, token: str, odata_base: str) -> dict:
    search_name = product_name if product_name.endswith(".SAFE") else f"{product_name}.SAFE"
    filter_query = f"Name eq '{search_name}'"
    url = f"{odata_base}/Products?$filter={filter_query}"

    try:
        resp = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=60)
    except requests.exceptions.RequestException as e:
        raise TransientIngestionError(f"OData query network error: {e}") from e

    if resp.status_code >= 500:
        raise TransientIngestionError(f"OData query returned {resp.status_code}")
    resp.raise_for_status()  # 4xx -> real HTTPError real

    results = resp.json().get("value", [])
    if not results:
        raise PermanentIngestionError(f"Product not found in OData: {search_name}")

    return results[0]