from __future__ import annotations

import logging

from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from app.models.storefront_audit_event import StorefrontAuditEvent

logger = logging.getLogger(__name__)


def record_storefront_audit(
    db: Session,
    *,
    brand: str,
    site_id: str | None,
    action: str,
    profile_id: str | None,
    redemption_id: str | None = None,
    request: dict | None = None,
    response: dict | None = None,
) -> None:
    try:
        payload_request = jsonable_encoder(request) if request is not None else None
        payload_response = jsonable_encoder(response) if response is not None else None
        event = StorefrontAuditEvent(
            brand=brand,
            site_id=site_id,
            action=action,
            profile_id=profile_id,
            redemption_id=redemption_id,
            request=payload_request,
            response=payload_response,
        )
        nested = getattr(db, "begin_nested", None)
        if callable(nested):
            with nested():
                db.add(event)
                db.flush()
        else:
            db.add(event)
            db.flush()
    except Exception:
        logger.exception(
            "storefront audit failed action=%s redemption_id=%s",
            action,
            redemption_id,
        )
