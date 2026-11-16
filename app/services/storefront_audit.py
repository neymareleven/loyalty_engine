from __future__ import annotations

from sqlalchemy.orm import Session

from app.models.storefront_audit_event import StorefrontAuditEvent


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
    db.add(
        StorefrontAuditEvent(
            brand=brand,
            site_id=site_id,
            action=action,
            profile_id=profile_id,
            redemption_id=redemption_id,
            request=request,
            response=response,
        )
    )
    db.flush()
