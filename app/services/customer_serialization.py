from __future__ import annotations

from sqlalchemy.orm import Session

from app.models.customer import Customer
from app.models.loyalty_tier import LoyaltyTier
from app.schemas.customer import CustomerOut
from app.services.birthdate_targeting import format_customer_birthdate_wire
from app.services.wallet_service import get_status_points_balance


def _format_birthdate(customer: Customer) -> str | None:
    return format_customer_birthdate_wire(customer)


def _tier_name_for_customer(db: Session, *, brand: str, customer: Customer) -> str | None:
    if not customer.loyalty_status:
        return None
    return (
        db.query(LoyaltyTier.name)
        .filter(LoyaltyTier.brand == brand)
        .filter(LoyaltyTier.key == customer.loyalty_status)
        .scalar()
    )


def serialize_customer_out(
    db: Session,
    *,
    customer: Customer,
    brand: str | None = None,
    include_points_balance: bool = True,
    tier_name: str | None = None,
    extra: dict | None = None,
) -> dict:
    brand = brand or customer.brand
    data = CustomerOut.model_validate(customer).model_dump()
    data["birthdate"] = _format_birthdate(customer)
    if tier_name is None:
        tier_name = _tier_name_for_customer(db, brand=brand, customer=customer)
    data["loyalty_status_name"] = tier_name
    if include_points_balance:
        data["points_balance"] = get_status_points_balance(db, customer.id)
    if extra:
        data.update(extra)
    return data
