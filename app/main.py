"""
FastAPI Proxy Demo
Proxies requests to jsonplaceholder.typicode.com with:
  - In-memory caching (30s TTL on GET responses)
  - Per-IP rate limiting (10 req/min)
  - Request logging with latency
  - Injected response headers (cache status, request ID, proxy identity)
  - /health and /metrics endpoints
"""

import time
import uuid
import logging
from collections import defaultdict
from typing import Any

import httpx
from cachetools import TTLCache
from fastapi import FastAPI, Request, Response, HTTPException
from fastapi.responses import JSONResponse

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

UPSTREAM = "https://jsonplaceholder.typicode.com"
CACHE_TTL = 30          # seconds
CACHE_MAXSIZE = 256
RATE_LIMIT = 10         # requests per window
RATE_WINDOW = 60        # seconds

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

cache: TTLCache = TTLCache(maxsize=CACHE_MAXSIZE, ttl=CACHE_TTL)
rate_buckets: dict[str, list[float]] = defaultdict(list)

metrics: dict[str, Any] = {
    "requests_total": 0,
    "cache_hits": 0,
    "cache_misses": 0,
    "rate_limited": 0,
    "upstream_errors": 0,
}

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("proxy")

# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="FastAPI Proxy Demo",
    description="A proxy in front of jsonplaceholder.typicode.com",
    version="1.0.0",
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _client_ip(request: Request) -> str:
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def _is_rate_limited(ip: str) -> bool:
    now = time.time()
    window_start = now - RATE_WINDOW
    hits = rate_buckets[ip]
    # Expire old entries
    rate_buckets[ip] = [t for t in hits if t > window_start]
    if len(rate_buckets[ip]) >= RATE_LIMIT:
        return True
    rate_buckets[ip].append(now)
    return False


def _cache_key(method: str, path: str, query: str) -> str:
    return f"{method}:{path}?{query}"


# ---------------------------------------------------------------------------
# Middleware — request logging
# ---------------------------------------------------------------------------


@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.perf_counter()
    request_id = str(uuid.uuid4())[:8]
    response = await call_next(request)
    elapsed_ms = (time.perf_counter() - start) * 1000
    log.info(
        "%s %s %s → %d (%.1fms)",
        request_id,
        request.method,
        request.url.path,
        response.status_code,
        elapsed_ms,
    )
    response.headers["X-Request-Id"] = request_id
    response.headers["X-Proxy-By"] = "fastapi-proxy-demo"
    return response


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.get("/health")
async def health():
    return {"status": "ok", "upstream": UPSTREAM}


@app.get("/metrics")
async def get_metrics():
    return {
        **metrics,
        "cache_size": len(cache),
        "cache_maxsize": CACHE_MAXSIZE,
        "cache_ttl_seconds": CACHE_TTL,
        "rate_limit": f"{RATE_LIMIT} req / {RATE_WINDOW}s",
    }


@app.api_route(
    "/proxy/{path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE"],
)
async def proxy(path: str, request: Request):
    ip = _client_ip(request)
    metrics["requests_total"] += 1

    # --- Rate limiting ---
    if _is_rate_limited(ip):
        metrics["rate_limited"] += 1
        raise HTTPException(
            status_code=429,
            detail=f"Rate limit exceeded: {RATE_LIMIT} requests per {RATE_WINDOW}s",
        )

    method = request.method
    query = request.url.query
    upstream_url = f"{UPSTREAM}/{path}" + (f"?{query}" if query else "")

    # --- Cache (GET only) ---
    if method == "GET":
        key = _cache_key(method, path, query)
        if key in cache:
            metrics["cache_hits"] += 1
            cached = cache[key]
            return JSONResponse(
                content=cached["body"],
                status_code=cached["status"],
                headers={"X-Cache": "HIT"},
            )
        metrics["cache_misses"] += 1

    # --- Forward to upstream ---
    body = await request.body()
    headers = {
        k: v
        for k, v in request.headers.items()
        if k.lower() not in ("host", "content-length")
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            upstream_resp = await client.request(
                method=method,
                url=upstream_url,
                headers=headers,
                content=body,
            )
    except httpx.RequestError as exc:
        metrics["upstream_errors"] += 1
        raise HTTPException(status_code=502, detail=f"Upstream error: {exc}")

    response_body = upstream_resp.json() if upstream_resp.content else None

    # Store in cache for GET
    if method == "GET" and upstream_resp.is_success:
        cache[key] = {"body": response_body, "status": upstream_resp.status_code}

    return JSONResponse(
        content=response_body,
        status_code=upstream_resp.status_code,
        headers={"X-Cache": "MISS" if method == "GET" else "BYPASS"},
    )
