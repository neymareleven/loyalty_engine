import csv
import io
from datetime import date

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from sqlalchemy.orm import Session

from app.db import get_db
from app.deps.brand import get_active_brand
from app.services.contact_service import get_or_create_customer


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

    rows: list[tuple[int, dict]] = []

    for idx, row in enumerate(reader, start=2):
        processed += 1
        rows.append((idx, row))

    for idx, row in rows:
        try:
            brand_in = (row.get("brand") or "").strip()
            if not brand_in:
                raise ValueError("brand is required")
            if brand_in != active_brand:
                raise ValueError("brand does not match active brand context")

            profile_id = (row.get("profileId") or row.get("profile_id") or "").strip()
            if not profile_id:
                raise ValueError("profileId is required")

            gender = (row.get("gender") or "").strip() or None
            birthdate = _parse_date((row.get("birthdate") or "").strip() or None)
        except Exception as e:
            errors.append({"line": idx, "error": str(e), "row": row})

    if errors:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "Import rejected: CSV validation failed. No customers were imported.",
                "processed": processed,
                "errors": errors,
            },
        )

    for idx, row in rows:
        brand = (row.get("brand") or "").strip()
        profile_id = (row.get("profileId") or row.get("profile_id") or "").strip()
        gender = (row.get("gender") or "").strip() or None
        birthdate = _parse_date((row.get("birthdate") or "").strip() or None)

        get_or_create_customer(
            db,
            brand,
            profile_id,
            {"gender": gender, "birthdate": birthdate},
        )
        created_or_updated += 1

    db.commit()

    return {
        "processed": processed,
        "upserted": created_or_updated,
        "errors": [],
    }
