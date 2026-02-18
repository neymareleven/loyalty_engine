from pydantic import BaseModel
from typing import Optional, Dict, Any


class EventCreate(BaseModel):
    brand: str
    profileId: str
    eventType: str
    eventId: str
    source: Optional[str] = "API"
    payload: Optional[Dict[str, Any]] = None
