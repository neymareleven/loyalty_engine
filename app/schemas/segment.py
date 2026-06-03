from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID

from pydantic import BaseModel


class SegmentCreate(BaseModel):
    brand: Optional[str] = None

    name: str
    description: Optional[str] = None

    is_dynamic: bool = True
    """Loyalty AST (INTERNAL) or loyalty AST translated to Unomi when provider=UNOMI."""
    conditions: Optional[Dict[str, Any]] = None
    """Optional raw Unomi condition JSON; if set on UNOMI dynamic create, used instead of translating conditions."""
    unomi_condition: Optional[Dict[str, Any]] = None

    active: bool = True


class SegmentUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None

    is_dynamic: Optional[bool] = None
    conditions: Optional[Dict[str, Any]] = None
    unomi_condition: Optional[Dict[str, Any]] = None

    active: Optional[bool] = None


class SegmentRecomputeResult(BaseModel):
    brand: str
    segments: int
    members: int
    computed_at: datetime


class SegmentOut(BaseModel):
    id: UUID
    brand: str

    name: str
    description: Optional[str] = None

    is_dynamic: bool
    conditions: Optional[Dict[str, Any]] = None

    provider: str = "INTERNAL"
    unomi_segment_id: Optional[str] = None
    unomi_scope: Optional[str] = None
    manual_profile_ids: list[str] = []
    unomi_condition: Optional[Dict[str, Any]] = None

    active: bool
    last_computed_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    member_count: int = 0
    member_count_dynamic: int = 0
    member_count_static: int = 0
    referencing_rules_count: int = 0
    referencing_internal_jobs_count: int = 0
    can_delete: bool = True
    recommended_action: Optional[str] = None
    needs_recompute: bool = False
    """loyalty_ast | unomi_only | null — CDP segments may only expose unomi_condition until reverse-translated."""
    conditions_format: Optional[str] = None

    class Config:
        from_attributes = True


class SegmentListAppliedFilters(BaseModel):
    q: Optional[str] = None
    is_dynamic: Optional[bool] = None
    provider: Optional[str] = None
    active: Optional[bool] = None


class SegmentListAppliedSort(BaseModel):
    sort_by: str = "created_at"
    sort_order: str = "desc"


class SegmentListResponse(BaseModel):
    items: list[SegmentOut]
    total: int
    limit: int
    offset: int
    filters: SegmentListAppliedFilters
    sort: SegmentListAppliedSort


class SegmentMembersBulkAdd(BaseModel):
    customer_ids: list[UUID]


class SegmentMembersBulkRemove(BaseModel):
    customer_ids: list[UUID]


class SegmentMembersBulkResult(BaseModel):
    created: int
    skipped_existing: int
    deleted: int
    missing: int
    invalid: int
    errors: list[dict]


class SegmentMemberCreate(BaseModel):
    customer_id: UUID


class SegmentMemberOut(BaseModel):
    segment_id: UUID
    customer_id: UUID
    source: str
    computed_at: Optional[datetime] = None
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class SegmentMemberListItem(BaseModel):
    segment_id: UUID
    customer_id: UUID | None = None
    profile_id: str | None = None
    source: str
    computed_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    membership_origin: Optional[str] = None
    customer_found_in_engine: bool | None = None
    """Dynamic segments only: live AST check at read time (use with ?verify=true)."""
    matches_conditions: Optional[bool] = None


class SegmentMembersListResponse(BaseModel):
    segment_id: UUID
    provider: str
    is_dynamic: bool
    unomi_segment_id: str | None = None
    total: int
    limit: int
    offset: int
    items: list[SegmentMemberListItem]
    note: str | None = None
    last_computed_at: Optional[datetime] = None
    membership_stale: bool = False
    refreshed: bool = False
    verified: bool = False
    page_mismatch_count: int = 0
