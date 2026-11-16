from __future__ import annotations

import secrets

from sqlalchemy.orm import Session

from app.models.customer_coupon import CustomerCoupon

_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"


def generate_coupon_code() -> str:
    chunk = lambda: "".join(secrets.choice(_ALPHABET) for _ in range(4))
    return f"LOY-{chunk()}-{chunk()}"


def allocate_unique_coupon_code(db: Session, *, attempts: int = 12) -> str:
    for _ in range(attempts):
        code = generate_coupon_code()
        exists = db.query(CustomerCoupon.id).filter(CustomerCoupon.code == code).first()
        if not exists:
            return code
    raise RuntimeError("Unable to allocate a unique coupon code")
