# Per-request httpx client factory (no shared connection pool)
import httpx

# Timeout for forwarded requests (seconds)
BACKEND_TIMEOUT = httpx.Timeout(timeout=300.0, connect=10.0)


def new_http_client() -> httpx.AsyncClient:
    """
    Return a brand-new httpx.AsyncClient for a single request.

    The CALLER owns the lifecycle and must use:
        async with new_http_client() as client:
            ...

    No connection is reused across calls. max_keepalive_connections=0
    makes "no connection reuse" explicit.
    """
    return httpx.AsyncClient(
        timeout=BACKEND_TIMEOUT,
        follow_redirects=True,
        limits=httpx.Limits(max_connections=1, max_keepalive_connections=0),
    )
