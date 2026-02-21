from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.db import get_db
from app.deps.brand import get_active_brand
from app.models.transaction import Transaction
from app.models.transaction_rule_execution import TransactionRuleExecution
from app.schemas.event import EventCreate
from app.schemas.transaction import TransactionOut
from app.schemas.execution import RuleExecutionOut
from app.services.transaction_service import create_transaction


router = APIRouter(prefix="/transactions", tags=["transactions"])


@router.post("")
def ingest_transaction(
    event: EventCreate,
    db: Session = Depends(get_db),
):
    transaction = create_transaction(db, event)
    return {
        "transactionId": str(transaction.id),
        "status": transaction.status,
    }


@router.get("", response_model=list[TransactionOut])
def list_transactions(
    active_brand: str = Depends(get_active_brand),
    brand: str | None = None,
    profileId: str | None = None,
    status: str | None = None,
    limit: int = 50,
    offset: int = 0,
    db: Session = Depends(get_db),
):
    q = db.query(Transaction)
    if brand and brand != active_brand:
        raise HTTPException(status_code=400, detail="brand does not match active brand context")
    q = q.filter(Transaction.brand == active_brand)
    if profileId:
        q = q.filter(Transaction.profile_id == profileId)
    if status:
        q = q.filter(Transaction.status == status)

    limit = max(1, min(limit, 200))
    offset = max(0, offset)

    return (
        q.order_by(Transaction.created_at.desc())
        .offset(offset)
        .limit(limit)
        .all()
    )


@router.get("/{transaction_id}", response_model=TransactionOut)
def get_transaction(
    transaction_id: str,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    tx = (
        db.query(Transaction)
        .filter(Transaction.id == transaction_id)
        .filter(Transaction.brand == active_brand)
        .first()
    )
    if not tx:
        raise HTTPException(status_code=404, detail="Transaction not found")
    return tx


@router.get("/{transaction_id}/executions", response_model=list[RuleExecutionOut])
def get_transaction_executions(
    transaction_id: str,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    tx = (
        db.query(Transaction.id)
        .filter(Transaction.id == transaction_id)
        .filter(Transaction.brand == active_brand)
        .first()
    )
    if not tx:
        raise HTTPException(status_code=404, detail="Transaction not found")

    executions = (
        db.query(TransactionRuleExecution)
        .filter(TransactionRuleExecution.transaction_id == transaction_id)
        .order_by(TransactionRuleExecution.executed_at.asc())
        .all()
    )
    return executions
