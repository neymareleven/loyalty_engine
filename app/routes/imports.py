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
