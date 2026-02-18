from datetime import datetime
from typing import Any, Dict, Optional

from uuid import UUID

from pydantic import BaseModel


class TransactionOut(BaseModel):
    id: UUID
    brand: str
    profile_id: str
    event_type: str
    event_id: str

    source: Optional[str] = None
    payload: Optional[Dict[str, Any]] = None

    status: str

    idempotency_key: Optional[str] = None
    error_code: Optional[str] = None
    error_message: Optional[str] = None

    created_at: Optional[datetime] = None
    processed_at: Optional[datetime] = None

    class Config:
        from_attributes = True
