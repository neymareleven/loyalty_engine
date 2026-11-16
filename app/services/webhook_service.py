from __future__ import annotations

import hashlib
import hmac
import json
import logging
import urllib.error
import urllib.request
from datetime import datetime, timedelta

from sqlalchemy.orm import Session

from app.models.webhook_endpoint import WebhookEndpoint
from app.models.webhook_outbox import WebhookOutbox

logger = logging.getLogger(__name__)

WEBHOOK_EVENTS = (
    "coupon.issued",
    "coupon.used",
    "coupon.expired",
    "tier.changed",
)
SIGNATURE_HEADER = "X-Loyalty-Signature"
MAX_ATTEMPTS = 8


def sign_payload(secret: str, body: bytes) -> str:
    digest = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def enqueue_webhook_event(db: Session, *, brand: str, event: str, payload: dict) -> int:
    if event not in WEBHOOK_EVENTS:
        return 0
    endpoints = (
        db.query(WebhookEndpoint)
        .filter(WebhookEndpoint.brand == brand)
        .filter(WebhookEndpoint.active.is_(True))
        .all()
    )
    queued = 0
    now = datetime.utcnow()
    for endpoint in endpoints:
        events = endpoint.events if isinstance(endpoint.events, list) else []
        if events and event not in events:
            continue
        db.add(
            WebhookOutbox(
                brand=brand,
                endpoint_id=endpoint.id,
                event=event,
                payload=payload,
                status="PENDING",
                attempts=0,
                next_attempt_at=now,
            )
        )
        queued += 1
    if queued:
        db.flush()
    return queued


def dispatch_due_webhooks(db: Session, *, brand: str | None = None, limit: int = 50) -> int:
    now = datetime.utcnow()
    q = (
        db.query(WebhookOutbox)
        .filter(WebhookOutbox.status == "PENDING")
        .filter((WebhookOutbox.next_attempt_at.is_(None)) | (WebhookOutbox.next_attempt_at <= now))
        .order_by(WebhookOutbox.created_at.asc())
        .limit(limit)
    )
    if brand:
        q = q.filter(WebhookOutbox.brand == brand)
    rows = q.all()
    sent = 0
    for row in rows:
        endpoint = db.query(WebhookEndpoint).filter(WebhookEndpoint.id == row.endpoint_id).first()
        if not endpoint or not endpoint.active:
            row.status = "FAILED"
            row.last_error = "endpoint missing or inactive"
            continue
        body = json.dumps(
            {"event": row.event, "brand": row.brand, "payload": row.payload},
            default=str,
            separators=(",", ":"),
        ).encode("utf-8")
        signature = sign_payload(endpoint.secret, body)
        req = urllib.request.Request(
            endpoint.url,
            data=body,
            method="POST",
            headers={
                "Content-Type": "application/json",
                SIGNATURE_HEADER: signature,
            },
        )
        row.attempts = int(row.attempts or 0) + 1
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                if 200 <= getattr(resp, "status", 200) < 300:
                    row.status = "SENT"
                    row.sent_at = datetime.utcnow()
                    row.last_error = None
                    sent += 1
                    continue
                raise RuntimeError(f"HTTP {getattr(resp, 'status', '?')}")
        except Exception as exc:
            row.last_error = str(exc)[:2000]
            if row.attempts >= MAX_ATTEMPTS:
                row.status = "FAILED"
            else:
                delay_minutes = 2 ** min(row.attempts, 6)
                row.next_attempt_at = datetime.utcnow() + timedelta(minutes=delay_minutes)
                logger.warning("webhook %s attempt %s failed: %s", row.id, row.attempts, exc)
    db.flush()
    return sent
