from __future__ import annotations

import hashlib
import hmac
import secrets
from datetime import datetime

from fastapi import Depends, HTTPException, Request
from sqlalchemy.orm import Session

from app.db import get_db
from app.models.api_client import ApiClient

STOREFRONT_SCOPE = "storefront"


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
    if STOREFRONT_SCOPE not in scopes and scopes:
        raise HTTPException(status_code=403, detail="Insufficient scope")
    client.last_used_at = datetime.utcnow()
    request.state.api_client = client
    request.state.brand = client.brand
    return client


def require_idempotency_key(request: Request) -> str:
    key = (request.headers.get("Idempotency-Key") or "").strip()
    if not key:
        raise HTTPException(status_code=400, detail="Idempotency-Key header is required")
    return key


def constant_time_equals(a: str, b: str) -> bool:
    if not a or not b:
        return False
    return hmac.compare_digest(a, b)
