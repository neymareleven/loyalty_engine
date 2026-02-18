from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.db import get_db
from app.models.customer import Customer
from app.services.wallet_service import get_points_balance

router = APIRouter(prefix="/wallet", tags=["wallet"])


@router.get("/{brand}/{profile_id}")
def read_wallet(brand: str, profile_id: str, db: Session = Depends(get_db)):

    customer = (
        db.query(Customer)
        .filter(Customer.brand == brand, Customer.profile_id == profile_id)
        .first()
    )

    if not customer:
        raise HTTPException(status_code=404, detail="Customer not found")

    balance = get_points_balance(db, customer.id)

    return {
        "brand": brand,
        "profileId": profile_id,
        "pointsBalance": balance,
    }
