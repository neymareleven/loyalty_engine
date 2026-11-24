from __future__ import annotations

import hashlib
import hmac
import logging
import os
import secrets
import threading
import time
from collections import defaultdict, deque
from datetime import datetime

from fastapi import Depends, HTTPException, Request
from sqlalchemy.orm import Session

from app.db import get_db
from app.models.api_client import ApiClient

STOREFRONT_SCOPE = "storefront"
STOREFRONT_ANONYMIZE_SCOPE = "storefront:anonymize"
_RATE_LIMIT_PER_MINUTE = 60
# In-process only: each uvicorn/gunicorn worker keeps its own counter.
_rate_lock = threading.Lock()
_rate_hits: dict[str, deque[float]] = defaultdict(deque)
logger = logging.getLogger(__name__)


def storefront_rate_limit_per_minute(client: ApiClient) -> int:
    override = getattr(client, "rate_limit_per_minute", None)
    if override is None:
        scopes = client.scopes if isinstance(getattr(client, "scopes", None), list) else []
        for item in scopes:
            if isinstance(item, str) and item.startswith("rl:"):
                override = item[3:]
                break
    env = os.getenv("STOREFRONT_RATE_LIMIT_PER_MINUTE")
    raw = override if override is not None else env
    try:
        value = int(raw) if raw is not None and raw != "" else _RATE_LIMIT_PER_MINUTE
    except (TypeError, ValueError):
        value = _RATE_LIMIT_PER_MINUTE
    return max(1, min(value, 10_000))


def hash_api_key(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def generate_api_key() -> str:
    return "le_live_" + secrets.token_urlsafe(32)


def require_storefront_client(request: Request, db: Session = Depends(get_db)) -> ApiClient:
    auth = request.headers.get("authorization") or ""
    if not auth.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Not authenticated", headers={"WWW-Authenticate": "Bearer"})
    token = auth.split(" ", 1)[1].strip()
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    digest = hash_api_key(token)
    client = (
        db.query(ApiClient)
        .filter(ApiClient.key_hash == digest)
        .filter(ApiClient.active.is_(True))
        .first()
    )
    if not client:
        raise HTTPException(status_code=401, detail="Invalid API key")
    scopes = client.scopes if isinstance(client.scopes, list) else []
    if STOREFRONT_SCOPE not in scopes:
        raise HTTPException(status_code=403, detail="Insufficient scope")
    now = datetime.utcnow()
    last = client.last_used_at
    if last is None or (now - last).total_seconds() >= 60:
        client.last_used_at = now
        request.state.storefront_dirty = True
    else:
        request.state.storefront_dirty = False
    request.state.api_client = client
    request.state.brand = client.brand
    return client


def require_storefront_anonymize_scope(client: ApiClient) -> None:
    scopes = client.scopes if isinstance(client.scopes, list) else []
    if STOREFRONT_ANONYMIZE_SCOPE not in scopes:
        raise HTTPException(status_code=403, detail="Insufficient scope (storefront:anonymize required)")


def enforce_storefront_rate_limit(client: ApiClient) -> None:
    key = str(client.id)
    now = time.monotonic()
    window_start = now - 60
    with _rate_lock:
        hits = _rate_hits[key]
        while hits and hits[0] < window_start:
            hits.popleft()
        if len(hits) >= storefront_rate_limit_per_minute(client):
            logger.warning("storefront rate limit exceeded client=%s brand=%s", client.id, client.brand)
            raise HTTPException(status_code=429, detail="Rate limit exceeded")
        hits.append(now)


def persist_storefront_session(db: Session, request: Request | None = None, *, force: bool = False) -> None:
    if force or (request is not None and getattr(request.state, "storefront_dirty", False)):
        db.commit()


def require_idempotency_key(request: Request) -> str:
    key = (request.headers.get("Idempotency-Key") or "").strip()
    if not key:
        raise HTTPException(status_code=400, detail="Idempotency-Key header is required")
    return key


def constant_time_equals(a: str, b: str) -> bool:
    if not a or not b:
        return False
    return hmac.compare_digest(a, b)
