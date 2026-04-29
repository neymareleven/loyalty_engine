from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.db import get_db
from app.models.transaction_rule_execution import TransactionRuleExecution
from app.models.event_type import TransactionType
from app.models.rule import Rule
from app.models.segment import Segment
from app.schemas.rule import RuleCreate, RuleOut, RuleUpdate, RuleReorderRequest
from app.deps.brand import get_active_brand


router = APIRouter(prefix="/rules", tags=["rules"])


_DEPRECATED_ACTION_TYPES = {"burn_points", "issue_reward", "use_coupon", "set_rank"}
_ALLOWED_ACTION_TYPES = {"earn_points", "issue_coupon", "reset_status_points"}


def _normalize_transaction_types(*, transaction_type: str | None, transaction_types: list[str] | None) -> list[str]:
    if transaction_types is not None:
        if not isinstance(transaction_types, list) or len(transaction_types) == 0:
            raise HTTPException(status_code=400, detail="transaction_types must be a non-empty list")
        items = []
        for t in transaction_types:
            if not isinstance(t, str) or not t.strip():
                raise HTTPException(status_code=400, detail="transaction_types must contain non-empty strings")
            items.append(t.strip())
        # dedupe while preserving order
        seen = set()
        out = []
        for t in items:
            if t not in seen:
                out.append(t)
                seen.add(t)
        return out

    if transaction_type is None or not str(transaction_type).strip():
        raise HTTPException(status_code=400, detail="transaction_type (legacy) or transaction_types is required")
    return [str(transaction_type).strip()]


def _validate_transaction_types_exist(db: Session, *, brand: str, keys: list[str]) -> None:
    if not keys:
        raise HTTPException(status_code=400, detail="transaction_types must be provided")
    found = (
        db.query(TransactionType.key)
        .filter(TransactionType.brand == brand)
        .filter(TransactionType.active.is_(True))
        .filter(TransactionType.key.in_(keys))
        .all()
    )
    found_keys = {k for (k,) in found}
    missing = [k for k in keys if k not in found_keys]
    if missing:
        raise HTTPException(
            status_code=400,
            detail="Unknown or inactive transaction_type(s): " + ", ".join(missing) + ". Create them in /admin/transaction-types first.",
        )


def _validate_segments_exist(db: Session, *, brand: str, segment_ids: list[str]) -> None:
    if not segment_ids:
        return
    found = (
        db.query(Segment.id)
        .filter(Segment.brand == brand)
        .filter(Segment.id.in_(segment_ids))
        .all()
    )
    found_ids = {sid for (sid,) in found}
    missing = [str(s) for s in segment_ids if s not in found_ids]
    if missing:
        raise HTTPException(status_code=400, detail="Unknown segment_id(s): " + ", ".join(missing))


def _validate_rule_actions(actions):
    if actions is None:
        raise HTTPException(status_code=400, detail="Rules must define at least one action")
    if isinstance(actions, dict):
        actions = [actions]
    if not isinstance(actions, list) or len(actions) == 0:
        raise HTTPException(status_code=400, detail="Rules must define at least one action")

    for a in actions:
        if not isinstance(a, dict):
            raise HTTPException(status_code=400, detail="Invalid action: expected object")
        t = a.get("type")
        if not isinstance(t, str) or not t:
            raise HTTPException(status_code=400, detail="Invalid action: missing type")
        if t in _DEPRECATED_ACTION_TYPES:
            raise HTTPException(status_code=400, detail=f"Action type '{t}' is deprecated and not allowed")
        if t not in _ALLOWED_ACTION_TYPES:
            raise HTTPException(status_code=400, detail=f"Unknown action type: {t}")


def _next_priority_for_brand(db: Session, *, brand: str) -> int:
    max_priority = (
        db.query(func.max(Rule.priority))
        .filter(Rule.brand == brand)
        .scalar()
    )
    if max_priority is None:
        return 0
    return int(max_priority) + 1


@router.get("", response_model=list[RuleOut])
def list_rules(
    active_brand: str = Depends(get_active_brand),
    brand: str | None = None,
    transaction_type: str | None = None,
    db: Session = Depends(get_db),
):
    q = db.query(Rule)
    if brand and brand != active_brand:
        raise HTTPException(status_code=400, detail="brand does not match active brand context")
    q = q.filter(Rule.brand == active_brand)
    if transaction_type:
        q = q.filter(Rule.transaction_type == transaction_type)
    return q.order_by(Rule.priority.asc(), Rule.transaction_type.asc(), Rule.created_at.asc()).all()


@router.post("", response_model=RuleOut)
def create_rule(
    payload: RuleCreate,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    if payload.brand is not None and payload.brand != active_brand:
        raise HTTPException(status_code=400, detail="payload.brand does not match active brand context")

    normalized_types = _normalize_transaction_types(
        transaction_type=payload.transaction_type,
        transaction_types=payload.transaction_types,
    )
    _validate_transaction_types_exist(db, brand=active_brand, keys=normalized_types)

    dup = (
        db.query(Rule.id)
        .filter(Rule.brand == active_brand)
        .filter(func.lower(Rule.name) == func.lower(payload.name))
        .first()
    )
    if dup:
        raise HTTPException(status_code=400, detail="A rule with this name already exists for this brand")

    _validate_rule_actions(payload.actions)

    seg_ids = payload.segment_ids or []
    _validate_segments_exist(db, brand=active_brand, segment_ids=seg_ids)

    next_priority = _next_priority_for_brand(db, brand=active_brand)

    rule = Rule(
        brand=active_brand,
        name=payload.name,
        description=payload.description,
        transaction_type=normalized_types[0],
        transaction_types=normalized_types,
        priority=next_priority,
        segment_ids=seg_ids or None,
        conditions=payload.conditions,
        actions=payload.actions,
        active=payload.active,
    )
    db.add(rule)
    try:
        db.commit()
    except IntegrityError as e:
        db.rollback()
        orig = getattr(e, "orig", None)
        pgcode = getattr(orig, "pgcode", None)
        if str(pgcode or "") == "23505":
            raise HTTPException(
                status_code=409,
                detail="Rule priority conflict detected. Please retry.",
            )
        raise
    db.refresh(rule)
    return rule


@router.post("/reorder")
def reorder_rules(
    payload: RuleReorderRequest,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    rule_ids = payload.rule_ids or []
    if not rule_ids:
        raise HTTPException(status_code=400, detail="rule_ids is required")

    # Ensure list has no duplicates and exactly matches brand rule set.
    if len(rule_ids) != len(set(rule_ids)):
        raise HTTPException(status_code=400, detail="rule_ids contains duplicates")

    brand_rules = db.query(Rule).filter(Rule.brand == active_brand).all()
    if len(brand_rules) != len(rule_ids):
        raise HTTPException(status_code=400, detail="rule_ids must include all rules for the brand")

    brand_rule_ids = {r.id for r in brand_rules}
    payload_rule_ids = set(rule_ids)
    if brand_rule_ids != payload_rule_ids:
        raise HTTPException(status_code=400, detail="rule_ids does not match rules in this brand")

    rule_map = {r.id: r for r in brand_rules}

    # Two-step reorder to avoid transient UNIQUE collisions while swapping priorities.
    temp_base = 1_000_000
    for idx, rid in enumerate(rule_ids):
        rule_map[rid].priority = temp_base + idx

    try:
        db.flush()
        for idx, rid in enumerate(rule_ids):
            rule_map[rid].priority = idx
        db.commit()
    except IntegrityError as e:
        db.rollback()
        orig = getattr(e, "orig", None)
        pgcode = getattr(orig, "pgcode", None)
        if str(pgcode or "") == "23505":
            raise HTTPException(
                status_code=409,
                detail="Rule priorities conflict during reorder. Please refresh and retry.",
            )
        raise HTTPException(status_code=409, detail="Unable to reorder rules due to data conflict")

    return {"updated": len(rule_ids)}


@router.get("/{rule_id}", response_model=RuleOut)
def get_rule(
    rule_id: UUID,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    rule = (
        db.query(Rule)
        .filter(Rule.id == rule_id)
        .filter(Rule.brand == active_brand)
        .first()
    )
    if not rule:
        raise HTTPException(status_code=404, detail="Rule not found")
    return rule


@router.patch("/{rule_id}", response_model=RuleOut)
def update_rule(
    rule_id: UUID,
    payload: RuleUpdate,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    rule = (
        db.query(Rule)
        .filter(Rule.id == rule_id)
        .filter(Rule.brand == active_brand)
        .first()
    )
    if not rule:
        raise HTTPException(status_code=404, detail="Rule not found")

    data = payload.model_dump(exclude_unset=True)
    if "brand" in data and data["brand"] is not None and data["brand"] != active_brand:
        raise HTTPException(status_code=400, detail="payload.brand does not match active brand context")

    if "transaction_type" in data or "transaction_types" in data:
        next_types = _normalize_transaction_types(
            transaction_type=data.get("transaction_type", rule.transaction_type),
            transaction_types=data.get("transaction_types"),
        )
        _validate_transaction_types_exist(db, brand=active_brand, keys=next_types)
        rule.transaction_type = next_types[0]
        rule.transaction_types = next_types
        data.pop("transaction_type", None)
        data.pop("transaction_types", None)

    if "segment_ids" in data:
        seg_ids = data.get("segment_ids") or []
        _validate_segments_exist(db, brand=active_brand, segment_ids=seg_ids)
        rule.segment_ids = seg_ids or None
        data.pop("segment_ids", None)

    if "name" in data and data["name"] is not None:
        new_name = str(data["name"])
        dup = (
            db.query(Rule.id)
            .filter(Rule.brand == active_brand)
            .filter(func.lower(Rule.name) == func.lower(new_name))
            .filter(Rule.id != rule_id)
            .first()
        )
        if dup:
            raise HTTPException(status_code=400, detail="A rule with this name already exists for this brand")

    if "actions" in data:
        _validate_rule_actions(data.get("actions"))

    if "priority" in data:
        raise HTTPException(
            status_code=400,
            detail="Priority is managed automatically and cannot be edited manually.",
        )

    next_actions = data.get("actions") if "actions" in data else rule.actions
    _validate_rule_actions(next_actions)

    for k, v in data.items():
        if k == "brand":
            continue
        setattr(rule, k, v)

    try:
        db.commit()
    except IntegrityError as e:
        db.rollback()
        orig = getattr(e, "orig", None)
        pgcode = getattr(orig, "pgcode", None)
        if str(pgcode or "") == "23505":
            raise HTTPException(
                status_code=409,
                detail="A rule with this priority already exists for this brand.",
            )
        raise
    db.refresh(rule)
    return rule


@router.delete("/{rule_id}")
def delete_rule(
    rule_id: UUID,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    rule = (
        db.query(Rule)
        .filter(Rule.id == rule_id)
        .filter(Rule.brand == active_brand)
        .first()
    )
    if not rule:
        raise HTTPException(status_code=404, detail="Rule not found")

    # Defensive cleanup: DB constraint should be ON DELETE SET NULL, but in case migrations
    # were not applied or constraints differ, null out references explicitly.
    (
        db.query(TransactionRuleExecution)
        .filter(TransactionRuleExecution.rule_id == rule_id)
        .update({TransactionRuleExecution.rule_id: None}, synchronize_session=False)
    )

    db.delete(rule)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code=409,
            detail="Cannot delete rule due to existing references (apply latest DB migrations or remove dependent records).",
        )
    return {"deleted": True}


