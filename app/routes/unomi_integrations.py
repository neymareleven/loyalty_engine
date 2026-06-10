"""Inbound Unomi → loyalty integration endpoints (profile lifecycle)."""

from __future__ import annotations

import os

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.db import get_db
from app.services.customer_delete_service import delete_loyalty_customer
from app.services.unomi_profile_service import reset_profile_sync_source, set_profile_sync_source

router = APIRouter(prefix="/integrations/unomi", tags=["integrations-unomi"])


class UnomiProfileEvent(BaseModel):
    event: str = Field(..., description="profile_deleted | profile_deactivated")
    brand: str
    profile_id: str = Field(..., alias="profileId")
    scope: str | None = None

    model_config = {"populate_by_name": True}


def _verify_webhook_secret(request: Request) -> None:
    expected = (os.getenv("UNOMI_WEBHOOK_SECRET") or "").strip()
    if not expected:
        return
    provided = (request.headers.get("X-Unomi-Webhook-Secret") or "").strip()
    if provided != expected:
        raise HTTPException(status_code=401, detail="Invalid Unomi webhook secret")


@router.post("/profile-events")
def unomi_profile_event(
    payload: UnomiProfileEvent,
    request: Request,
    db: Session = Depends(get_db),
):
    """
    Called from Unomi when a profile is removed (groovy action / privacy API hook).

    Deletes the matching loyalty customer without re-deleting the Unomi profile.
    """
    _verify_webhook_secret(request)

    event = (payload.event or "").strip().lower()
    brand = payload.brand.strip()
    profile_id = payload.profile_id.strip()
    if not brand or not profile_id:
        raise HTTPException(status_code=400, detail="brand and profileId are required")

    if event not in {"profile_deleted", "profile_deactivated"}:
        raise HTTPException(status_code=400, detail="Unsupported event")

    token = set_profile_sync_source("unomi")
    try:
        result = delete_loyalty_customer(
            db,
            brand=brand,
            profile_id=profile_id,
            skip_unomi=True,
        )
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        reset_profile_sync_source(token)

    return result
