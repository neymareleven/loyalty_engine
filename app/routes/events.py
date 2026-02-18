from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.schemas.event import EventCreate
from app.services.transaction_service import create_transaction
from app.db import get_db

router = APIRouter()


@router.post("/events")
def create_event(event: EventCreate, db: Session = Depends(get_db)):
    # validation minimale déjà faite par Pydantic

    transaction = create_transaction(db, event)

    return {
        "transactionId": str(transaction.id),
        "status": transaction.status,
    }
