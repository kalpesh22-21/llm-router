# Shared httpx async client (connection pooling across all backends)
import httpx

_HTTP_CLIENT: httpx.AsyncClient | None = None

# Timeout for forwarded requests (seconds)
BACKEND_TIMEOUT = httpx.Timeout(timeout=300.0, connect=10.0)


def get_http_client() -> httpx.AsyncClient:
    """Return the module-level httpx client, creating it on first call."""
    global _HTTP_CLIENT
    if _HTTP_CLIENT is None or _HTTP_CLIENT.is_closed:
        _HTTP_CLIENT = httpx.AsyncClient(
            timeout=BACKEND_TIMEOUT,
            follow_redirects=True,
            limits=httpx.Limits(max_connections=200, max_keepalive_connections=50),
        )
    return _HTTP_CLIENT


async def close_http_client() -> None:
    """Close the shared httpx client (called during app shutdown)."""
    global _HTTP_CLIENT
    if _HTTP_CLIENT is not None and not _HTTP_CLIENT.is_closed:
        await _HTTP_CLIENT.aclose()
        _HTTP_CLIENT = None
