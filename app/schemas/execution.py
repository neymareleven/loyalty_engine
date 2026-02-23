from datetime import datetime
from typing import Any, Dict, Optional

from uuid import UUID

from pydantic import BaseModel


class RuleExecutionOut(BaseModel):
    id: UUID
    transaction_id: UUID
    rule_id: Optional[UUID] = None

    result: str
    details: Optional[Dict[str, Any]] = None

    executed_at: Optional[datetime] = None

    class Config:
        from_attributes = True
