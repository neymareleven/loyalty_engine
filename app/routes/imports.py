import csv
import io
import json
from datetime import date

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from sqlalchemy import tuple_
from sqlalchemy.orm import Session

from app.db import get_db
from app.deps.brand import get_active_brand
from app.models.customer import Customer
from app.schemas.event import EventCreate
from app.services.contact_service import get_or_create_customer
from app.services.transaction_service import create_transaction


router = APIRouter(prefix="/imports", tags=["imports"])


def _parse_date(value: str | None):
    if not value:
        return None
    return date.fromisoformat(value)


@router.post("/customers")
async def import_customers_csv(
    file: UploadFile = File(...),
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="CSV file is required")

    content = await file.read()
    text = content.decode("utf-8-sig")

    reader = csv.DictReader(io.StringIO(text))

    processed = 0
    created_or_updated = 0
    errors: list[dict] = []

    for idx, row in enumerate(reader, start=2):
        processed += 1
        try:
            brand_in = (row.get("brand") or "").strip()
            if brand_in and brand_in != active_brand:
                raise ValueError("brand does not match active brand context")
            brand = brand_in or active_brand
            profile_id = (row.get("profileId") or row.get("profile_id") or "").strip()
            gender = (row.get("gender") or "").strip() or None
            birthdate = _parse_date((row.get("birthdate") or "").strip() or None)

            if not profile_id:
                raise ValueError("profileId is required")

            get_or_create_customer(
                db,
                brand,
                profile_id,
                {"gender": gender, "birthdate": birthdate},
            )
            created_or_updated += 1
        except Exception as e:
            errors.append({"line": idx, "error": str(e), "row": row})

    db.commit()

    return {
        "processed": processed,
        "upserted": created_or_updated,
        "errors": errors,
    }


@router.post("/events")
async def import_events_csv(
    file: UploadFile = File(...),
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="CSV file is required")

    content = await file.read()
    text = content.decode("utf-8-sig")

    reader = csv.DictReader(io.StringIO(text))

    rows: list[tuple[int, dict]] = []
    customer_keys: set[tuple[str, str]] = set()

    for idx, row in enumerate(reader, start=2):
        rows.append((idx, row))
        brand_in = (row.get("brand") or "").strip()
        if brand_in and brand_in != active_brand:
            continue
        brand = brand_in or active_brand
        profile_id = (row.get("profileId") or row.get("profile_id") or "").strip()
        if brand and profile_id:
            customer_keys.add((brand, profile_id))

    # ------------------------------------------------------------
    # Pre-check: ensure all referenced customers exist before processing.
    # ------------------------------------------------------------
    missing_customers: list[dict] = []
    if customer_keys:
        existing = (
            db.query(Customer.brand, Customer.profile_id)
            .filter(tuple_(Customer.brand, Customer.profile_id).in_(customer_keys))
            .all()
        )
        existing_set = {(b, p) for (b, p) in existing}
        for brand, profile_id in sorted(customer_keys):
            if (brand, profile_id) not in existing_set:
                missing_customers.append({"brand": brand, "profileId": profile_id})

    if missing_customers:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "Missing customers. Import customers first using /imports/customers or /customers/upsert.",
                "missingCustomers": missing_customers,
            },
        )

    mismatched_brand_lines: list[int] = []
    for idx, row in rows:
        brand_in = (row.get("brand") or "").strip()
        if brand_in and brand_in != active_brand:
            mismatched_brand_lines.append(idx)
    if mismatched_brand_lines:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "Some rows have a brand that does not match active brand context.",
                "activeBrand": active_brand,
                "lines": mismatched_brand_lines,
            },
        )

    processed = 0
    succeeded = 0
    failed = 0
    results: list[dict] = []

    for idx, row in rows:
        processed += 1
        try:
            brand_in = (row.get("brand") or "").strip()
            if brand_in and brand_in != active_brand:
                raise ValueError("brand does not match active brand context")
            brand = brand_in or active_brand
            profile_id = (row.get("profileId") or row.get("profile_id") or "").strip()
            event_type = (row.get("eventType") or row.get("event_type") or "").strip()
            event_id = (row.get("eventId") or row.get("event_id") or "").strip()
            source = (row.get("source") or "IMPORT").strip() or "IMPORT"

            payload_raw = row.get("payload") or row.get("payload_json")
            payload = None
            if payload_raw and str(payload_raw).strip():
                payload = json.loads(payload_raw)

            if not payload:
                payload = {}

            # Convenience fields
            if row.get("amount") and "amount" not in payload:
                payload["amount"] = row.get("amount")
            if row.get("points") and "points" not in payload:
                payload["points"] = row.get("points")
            if row.get("rewardId") and "rewardId" not in payload:
                payload["rewardId"] = row.get("rewardId")

            event = EventCreate(
                brand=brand,
                profileId=profile_id,
                eventType=event_type,
                eventId=event_id,
                source=source,
                payload=payload,
            )

            tx = create_transaction(db, event)
            succeeded += 1
            results.append({"line": idx, "eventId": event_id, "transactionId": str(tx.id), "status": tx.status})
        except Exception as e:
            failed += 1
            results.append({"line": idx, "eventId": row.get("eventId") or row.get("event_id"), "error": str(e)})

    return {
        "processed": processed,
        "succeeded": succeeded,
        "failed": failed,
        "results": results,
    }
