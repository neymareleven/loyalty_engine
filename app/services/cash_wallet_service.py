from sqlalchemy.orm import Session
from sqlalchemy import func

from app.models.cash_movement import CashMovement


def get_cash_balances(db: Session, customer_id):
    rows = (
        db.query(CashMovement.currency, func.coalesce(func.sum(CashMovement.amount), 0))
        .filter(CashMovement.customer_id == customer_id)
        .group_by(CashMovement.currency)
        .all()
    )

    return {str(cur): int(total or 0) for cur, total in rows}


def credit_cash(db: Session, customer_id, *, amount: int, currency: str, source_transaction_id=None):
    try:
        amount = int(amount)
    except Exception:
        return None

    if amount <= 0:
        return None

    cur = (currency or "").strip().upper()
    if len(cur) != 3:
        return None

    movement = CashMovement(
        customer_id=customer_id,
        amount=amount,
        currency=cur,
        type="CREDIT",
        source_transaction_id=source_transaction_id,
    )

    db.add(movement)
    db.flush()
    return movement
