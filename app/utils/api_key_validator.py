"""
API Key Validator — discovers and validates multiple API keys from environment variables.

Discovers all environment variables ending with '_KEY', tests each against
the backend's /models endpoint, and returns the first working key.
"""

from __future__ import annotations

import logging

import httpx

from app.core.httpx import get_http_client

logger = logging.getLogger(__name__)


async def validate_api_key(base_url: str, api_key: str) -> bool:
    """
    Validate an API key by making a request to the /models endpoint.

    Args:
        base_url: The base URL of the backend (e.g., http://localhost:8000/v1)
        api_key: The API key to validate

    Returns:
        True if the key is valid (returns 200 from /models), False otherwise
    """
    url = f"{base_url.rstrip('/')}/models"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    try:
        client = get_http_client()
        response = await client.get(url, headers=headers, timeout=1.0)

        if response.status_code == 200:
            logger.debug(f"API key validation succeeded for {base_url}")
            return True
        elif response.status_code == 401:
            logger.debug(f"API key validation failed (401) for {base_url}")
            return False
        else:
            logger.warning(
                f"API key validation returned unexpected status {response.status_code} for {base_url}"
            )
            return False

    except Exception as e:
        logger.warning(f"API key validation error for {base_url}: {e}")
        raise e


async def find_working_api_key(base_url: str, API_Keys: dict) -> str:

    key_vars = [name for name in API_Keys.keys() if name.endswith("_KEY")]
    # Sort for deterministic behavior
    key_vars.sort()

    if not key_vars:
        logger.warning(f"No API key environment variables found (ending with '_KEY')")
        return ""

    logger.info(f"Testing {len(key_vars)} API keys for {base_url}...")

    for var_name in key_vars:
        api_key = API_Keys.get(var_name, "").strip()
        if not api_key:
            continue

        logger.debug(f"Trying {var_name} for {base_url}")

        if await validate_api_key(base_url, api_key):
            logger.info(f"Found working API key '{var_name}' for {base_url}")
            return api_key

    logger.warning(f"No working API key found for {base_url}")
    return ""
