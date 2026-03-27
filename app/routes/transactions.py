from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.db import get_db
from app.deps.brand import get_active_brand
from app.models.transaction import Transaction
from app.models.transaction_rule_execution import TransactionRuleExecution
from app.schemas.event import EventCreate, UnomiEventCreate
from app.schemas.transaction import TransactionOut
from app.schemas.execution import RuleExecutionOut
from app.services.transaction_service import create_transaction


router = APIRouter(prefix="/transactions", tags=["transactions"])


@router.post("")
def ingest_transaction(
    event: UnomiEventCreate,
    db: Session = Depends(get_db),
):
    if not (event.brand or "").strip():
        raise HTTPException(status_code=400, detail="brand is required")
    if not (event.profileId or "").strip():
        raise HTTPException(status_code=400, detail="profileId is required")
    if not (event.itemId or "").strip():
        raise HTTPException(status_code=400, detail="itemId is required")
    if not (event.eventType or "").strip():
        raise HTTPException(status_code=400, detail="eventType is required")

    payload = event.properties or {}
    payload_brand = payload.get("brand")
    if isinstance(payload_brand, str) and payload_brand.strip() and payload_brand.strip() != event.brand:
        raise HTTPException(status_code=400, detail="properties.brand must match brand")

    mapped = EventCreate(
        brand=event.brand,
        profileId=event.profileId,
        eventType=event.eventType,
        eventId=event.itemId,
        source="UNOMI",
        payload=payload,
    )

    transaction = create_transaction(db, mapped)
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


@router.get("/by-user", response_model=list[TransactionOut])
def list_transactions_by_user(
    brand: str,
    profileId: str,
    active_brand: str = Depends(get_active_brand),
    limit: int = 50,
    offset: int = 0,
    db: Session = Depends(get_db),
):
    if brand != active_brand:
        raise HTTPException(status_code=400, detail="brand does not match active brand context")

    q = (
        db.query(Transaction)
        .filter(Transaction.brand == active_brand)
        .filter(Transaction.profile_id == profileId)
    )

    limit = max(1, min(limit, 200))
    offset = max(0, offset)

    return (
        q.order_by(Transaction.created_at.desc())
        .offset(offset)
        .limit(limit)
        .all()
    )


@router.get("/by-user-and-type", response_model=list[TransactionOut])
def list_transactions_by_user_and_type(
    brand: str,
    transactionType: str,
    profileId: str,
    active_brand: str = Depends(get_active_brand),
    limit: int = 50,
    offset: int = 0,
    db: Session = Depends(get_db),
):
    if brand != active_brand:
        raise HTTPException(status_code=400, detail="brand does not match active brand context")

    q = (
        db.query(Transaction)
        .filter(Transaction.brand == active_brand)
        .filter(Transaction.profile_id == profileId)
        .filter(Transaction.transaction_type == transactionType)
    )

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
