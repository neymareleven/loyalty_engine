from pydantic import BaseModel
from typing import Optional, Dict, Any


class EventCreate(BaseModel):
    brand: str
    profileId: str
    eventType: str
    eventId: str
    source: Optional[str] = "API"
    payload: Optional[Dict[str, Any]] = None


class UnomiEventCreate(BaseModel):
    itemId: str
    brand: str
    eventType: str
    profileId: str
    properties: Optional[Dict[str, Any]] = None
