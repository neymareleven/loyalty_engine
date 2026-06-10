from typing import Any, Optional
from uuid import UUID

from pydantic import BaseModel, Field


class CatalogDeletePreviewOut(BaseModel):
    resource_type: str
    resource_id: UUID
    resource_name: str
    can_delete: bool = True
    recommended_action: Optional[str] = None
    counts: dict[str, int] = Field(default_factory=dict)
    effects: list[str] = Field(default_factory=list)
    message: str
