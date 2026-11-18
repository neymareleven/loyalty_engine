from types import SimpleNamespace
from uuid import UUID
import logging
import os

from fastapi import APIRouter, Body, Depends, HTTPException
from datetime import datetime
from sqlalchemy import func
from sqlalchemy.orm import Session
from pydantic import BaseModel

from app.db import get_db
from app.deps.brand import get_active_brand
from app.models.customer import Customer
from app.models.internal_job import InternalJob
from app.models.loyalty_tier import LoyaltyTier
from app.schemas.loyalty_tier import LoyaltyTierCreate, LoyaltyTierOut, LoyaltyTierUpdate

from app.services.loyalty_status_service import update_customer_status
from app.services.purchase_service import QUALIFICATION_PURCHASES, get_qualification_mode

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/admin/loyalty-tiers", tags=["admin-loyalty-tiers"])


class PreviewImpactIn(BaseModel):
    tier_id: UUID | None = None
    key: str | None = None
    name: str | None = None
    min_status_points: int | None = None
    min_purchase_count: int | None = None
    min_purchase_amount: int | None = None
    active: bool | None = True


class RecomputeCustomersIn(BaseModel):
    applyDowngrades: bool = False


@router.get("/ui-catalog")
def get_loyalty_tiers_ui_catalog():

    def _model_json_schema(model_cls):
        fn = getattr(model_cls, "model_json_schema", None)
        if callable(fn):
            return fn()
        return model_cls.schema()

    return {
        "jsonSchema": _model_json_schema(LoyaltyTierCreate),
        "uiHints": {
            "brand": {"widget": "hidden"},
            "key": {"widget": "hidden"},
            "name": {"widget": "text"},
            "min_status_points": {
                "widget": "number",
                "min": 0,
                "label": "Seuil points (mode POINTS)",
            },
            "min_purchase_count": {
                "widget": "number",
                "min": 0,
                "label": "Seuil nombre d'achats (mode PURCHASES)",
            },
            "min_purchase_amount": {
                "widget": "number",
                "min": 0,
                "label": "Seuil montant total (mode PURCHASES)",
            },
            "rank": {"widget": "hidden"},
            "active": {"widget": "switch"},
        },
        "examples": [
            {
                "name": "Silver",
                "min_status_points": 100,
                "min_purchase_count": 3,
                "min_purchase_amount": 50000,
                "active": True,
            }
        ],
        "notes": {
            "baseTier": (
                "Un tier Base (min_status_points=0, min_purchase_count=0, min_purchase_amount=0) "
                "est créé automatiquement."
            ),
            "qualificationMode": (
                "GET/PATCH /admin/loyalty-settings → qualification_mode=POINTS|PURCHASES. "
                "En mode POINTS, min_status_points est utilisé. "
                "En mode PURCHASES, le client doit atteindre min_purchase_count ET min_purchase_amount."
            ),
            "firstPurchaseBypass": (
                "GET/PATCH /admin/loyalty-settings → first_purchase_bypass_enabled, "
                "first_purchase_bypass_min_amount, first_purchase_bypass_tier_key. "
                "Si activé, le tout premier achat (transaction sale) d'un montant >= min_amount "
                "accorde le palier cible s'il existe déjà, sans attendre les seuils du mode. "
                "Un palier plus élevé déjà mérité par les seuils n'est pas abaissé."
            ),
            "rank": "Calculé automatiquement côté serveur — ne pas envoyer rank=0 sauf pour le tier de base.",
            "hideInactiveModeFields": (
                "Masquer les champs du mode inactif. En POINTS : n’afficher que min_status_points. "
                "En PURCHASES : n’afficher que min_purchase_count et min_purchase_amount. "
                "Omettre les champs cachés (ou les envoyer à 0) : le serveur aligne les seuils "
                "du mode inactif pour garder rank/ordre cohérents au changement de mode."
            ),
            "baseTierByMode": (
                "Le palier de base du mode POINTS a min_status_points=0. "
                "Le palier de base du mode PURCHASES a min_purchase_count=0 et min_purchase_amount=0. "
                "POST /admin/loyalty-tiers/ensure-base-tier crée ou réactive ce palier 0/0/0."
            ),
        },
    }


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


def _slug_key(value: str) -> str:
    s = (value or "").strip().lower()
    out = []
    prev_us = False
    for ch in s:
        ok = ("a" <= ch <= "z") or ("0" <= ch <= "9")
        if ok:
            out.append(ch)
            prev_us = False
        else:
            if not prev_us:
                out.append("_")
                prev_us = True
    key = "".join(out).strip("_")
    return key or "tier"


def _recompute_customers(db: Session, brand: str) -> dict:
    customers = db.query(Customer).filter(Customer.brand == brand).all()
    updated = 0
    for c in customers:
        before = c.loyalty_status
        update_customer_status(
            db,
            c,
            reason="AUTO_TIER_REFRESH",
            source_transaction_id=None,
            depth=0,
            refresh_window=False,
            emit_events=True,
        )
        if c.loyalty_status != before:
            updated += 1
    db.commit()
    return {"brand": brand, "customers": len(customers), "updated": updated}


def _enqueue_recompute_customers_job(
    db: Session,
    brand: str,
    *,
    apply_qualification_mode: bool = False,
    backfill_purchases: bool = False,
    backfill_after_id=None,
    apply_downgrades: bool = False,
) -> None:
    job = (
        db.query(InternalJob)
        .filter(InternalJob.job_key == "MAINT_RECOMPUTE_CUSTOMERS_LOYALTY_STATUS")
        .filter(InternalJob.brand == brand)
        .first()
    )
    if not job:
        job = InternalJob(
            job_key="MAINT_RECOMPUTE_CUSTOMERS_LOYALTY_STATUS",
            brand=brand,
            name="Maintenance: Recompute Customers Loyalty Status",
            description=None,
            transaction_type="MAINTENANCE",
            selector={"batch_size": 500},
            payload_template=None,
            active=True,
            schedule={"type": "cron", "cron": "*/1 * * * *", "timezone": "UTC"},
        )
        db.add(job)
        db.flush()

    from sqlalchemy.orm.attributes import flag_modified

    selector = dict(job.selector or {})
    selector.pop("after_id", None)
    selector.pop("backfill_after_id", None)
    selector.setdefault("batch_size", 500)
    if apply_qualification_mode:
        selector["apply_qualification_mode"] = True
    if backfill_purchases:
        selector["backfill_purchases"] = True
        if backfill_after_id:
            selector["backfill_after_id"] = str(backfill_after_id)
    else:
        selector.pop("backfill_purchases", None)
    if apply_downgrades:
        selector["apply_downgrades"] = True
    else:
        selector.pop("apply_downgrades", None)
    job.selector = selector
    flag_modified(job, "selector")
    job.active = True
    job.next_run_at = datetime.utcnow()
    db.commit()


def _tier_mode_order_key(tier: LoyaltyTier, mode: str) -> tuple:
    points = int(tier.min_status_points or 0)
    count = int(getattr(tier, "min_purchase_count", 0) or 0)
    amount = int(getattr(tier, "min_purchase_amount", 0) or 0)
    created = getattr(tier, "created_at", None)
    if mode == QUALIFICATION_PURCHASES:
        return (count, amount, points, created or datetime.min)
    return (points, count, amount, created or datetime.min)


def _recompute_tier_ranks(db: Session, brand: str) -> None:
    mode = get_qualification_mode(db, brand=brand)
    tiers = db.query(LoyaltyTier).filter(LoyaltyTier.brand == brand).all()
    active = [t for t in tiers if bool(t.active)]
    inactive = [t for t in tiers if not bool(t.active)]
    active.sort(key=lambda t: _tier_mode_order_key(t, mode))
    inactive.sort(key=lambda t: _tier_mode_order_key(t, mode))
    for i, t in enumerate(active):
        t.rank = i
    for i, t in enumerate(inactive):
        t.rank = len(active) + i
    db.flush()


def _align_inactive_mode_thresholds(db: Session, brand: str) -> list[dict]:
    """Keep hidden-mode thresholds unique and strictly increasing in the active-mode order.

    The palier order follows the active qualification mode. The other mode's minima are
    auto-adjusted so switching mode does not invert ranks.
    """
    mode = get_qualification_mode(db, brand=brand)
    tiers = db.query(LoyaltyTier).filter(LoyaltyTier.brand == brand).all()
    adjustments: list[dict] = []
    if not tiers:
        return adjustments
    ordered = sorted(tiers, key=lambda t: _tier_mode_order_key(t, mode))
    if mode == QUALIFICATION_PURCHASES:
        prev_points = -1
        for i, t in enumerate(ordered):
            points = int(t.min_status_points or 0)
            new_points = points
            if i == 0:
                new_points = 0
                prev_points = 0
            elif points <= prev_points:
                new_points = prev_points + 1
                prev_points = new_points
            else:
                prev_points = points
            if new_points != points:
                logger.info(
                    "adjusted inactive-mode threshold brand=%s key=%s min_status_points %s -> %s",
                    brand,
                    t.key,
                    points,
                    new_points,
                )
                adjustments.append(
                    {
                        "key": t.key,
                        "name": t.name,
                        "field": "min_status_points",
                        "from": points,
                        "to": new_points,
                    }
                )
                t.min_status_points = new_points
    db.flush()
    _recompute_tier_ranks(db, brand)
    return adjustments


def unique_min_status_points(used: set[int], desired: int) -> int:
    """Next value that satisfies uq_loyalty_tiers_brand_min_status_points."""
    desired = int(desired or 0)
    if desired not in used:
        return desired
    return (max(used) + 1) if used else 0


def _used_min_status_points(db: Session, brand: str, *, exclude_id: UUID | None = None) -> set[int]:
    q = db.query(LoyaltyTier.min_status_points).filter(LoyaltyTier.brand == brand)
    if exclude_id is not None:
        q = q.filter(LoyaltyTier.id != exclude_id)
    return {int(v[0] or 0) for v in q.all()}


def _initial_rank_for_new_tier(*, db: Session, brand: str, min_status_points: int) -> int:
    """Pick a rank valid for DB flush before _recompute_tier_ranks.

    Constraint ck_loyalty_tiers_rank0_min0: rank=0 requires min_status_points=0.
    """
    min_i = int(min_status_points)
    if min_i == 0:
        return 0
    existing_count = (
        db.query(func.count(LoyaltyTier.id)).filter(LoyaltyTier.brand == brand).scalar() or 0
    )
    return max(int(existing_count), 1)


def _as_non_negative_int(value, *, field: str) -> int:
    if value is None:
        raise HTTPException(status_code=400, detail=f"{field} is required")
    try:
        parsed = int(value)
    except Exception:
        raise HTTPException(status_code=400, detail=f"{field} must be a whole number")
    if parsed < 0:
        raise HTTPException(status_code=400, detail=f"{field} must be 0 or more")
    return parsed


def purchase_threshold_problems(rows: list[dict]) -> list[dict]:
    """rows: {name, count, amount, key?}. Detect non-monotonic purchase thresholds."""
    problems: list[dict] = []
    ordered = sorted(rows, key=lambda r: (int(r["count"]), int(r["amount"])))
    for prev, cur in zip(ordered, ordered[1:]):
        prev_pair = (int(prev["count"]), int(prev["amount"]))
        cur_pair = (int(cur["count"]), int(cur["amount"]))
        keys = [prev.get("key") or prev["name"], cur.get("key") or cur["name"]]
        if cur_pair == prev_pair:
            problems.append(
                {
                    "mode": QUALIFICATION_PURCHASES,
                    "message": (
                        f"Les niveaux « {prev['name']} » et « {cur['name']} » ont les mêmes seuils."
                    ),
                    "tiers": keys,
                }
            )
        elif int(cur["amount"]) < int(prev["amount"]):
            problems.append(
                {
                    "mode": QUALIFICATION_PURCHASES,
                    "message": (
                        f"« {cur['name']} » demande plus d'achats que « {prev['name']} » mais un montant plus faible "
                        f"({cur['amount']} contre {prev['amount']}). "
                        f"Le montant ne doit pas diminuer d'un niveau à l'autre."
                    ),
                    "tiers": keys,
                }
            )
    return problems


def purchase_threshold_errors(rows: list[dict]) -> list[str]:
    return [p["message"] for p in purchase_threshold_problems(rows)]


def points_threshold_problems(rows: list[dict]) -> list[dict]:
    problems: list[dict] = []
    ordered = sorted(rows, key=lambda r: int(r["min"]))
    for prev, cur in zip(ordered, ordered[1:]):
        if int(cur["min"]) <= int(prev["min"]):
            problems.append(
                {
                    "mode": "POINTS",
                    "message": (
                        f"Les niveaux « {prev['name']} » et « {cur['name']} » n'ont pas des seuils de points "
                        f"uniques et strictement croissants ({prev['min']} → {cur['min']})."
                    ),
                    "tiers": [prev.get("key") or prev["name"], cur.get("key") or cur["name"]],
                }
            )
    return problems


def diagnose_tier_consistency(db: Session, brand: str) -> dict:
    mode = get_qualification_mode(db, brand=brand)
    tiers = (
        db.query(LoyaltyTier)
        .filter(LoyaltyTier.brand == brand)
        .filter(LoyaltyTier.active.is_(True))
        .all()
    )
    problems: list[dict] = []
    point_rows = [
        {
            "name": t.name,
            "key": t.key,
            "min": int(t.min_status_points or 0),
        }
        for t in tiers
    ]
    if point_rows:
        ordered = sorted(point_rows, key=lambda r: r["min"])
        if ordered[0]["min"] != 0:
            problems.append(
                {
                    "mode": "POINTS",
                    "message": "Le niveau de base du mode Points doit avoir min_status_points=0.",
                    "tiers": [ordered[0].get("key") or ordered[0]["name"]],
                }
            )
        problems.extend(points_threshold_problems(point_rows))
    purchase_rows = [
        {
            "name": t.name,
            "key": t.key,
            "count": int(getattr(t, "min_purchase_count", 0) or 0),
            "amount": int(getattr(t, "min_purchase_amount", 0) or 0),
        }
        for t in tiers
    ]
    if purchase_rows:
        ordered = sorted(purchase_rows, key=lambda r: (r["count"], r["amount"]))
        if ordered[0]["count"] != 0 or ordered[0]["amount"] != 0:
            problems.append(
                {
                    "mode": QUALIFICATION_PURCHASES,
                    "message": (
                        "Le niveau de base du mode Achats doit avoir "
                        "min_purchase_count=0 et min_purchase_amount=0."
                    ),
                    "tiers": [ordered[0].get("key") or ordered[0]["name"]],
                }
            )
        problems.extend(purchase_threshold_problems(purchase_rows))
    return {"mode": mode, "ok": not problems, "problems": problems}


def _tier_out(obj: LoyaltyTier, adjustments: list[dict] | None = None) -> LoyaltyTierOut:
    data = LoyaltyTierOut.model_validate(obj)
    if adjustments:
        return data.model_copy(update={"adjustedInactiveModeThresholds": adjustments})
    return data


def _simulated_tier_row(t, *, overrides: dict | None = None) -> SimpleNamespace:
    o = overrides or {}
    return SimpleNamespace(
        key=o.get("key", t.key),
        name=o.get("name", t.name),
        active=bool(o["active"]) if "active" in o else bool(t.active),
        min_status_points=int(o.get("min_status_points", t.min_status_points) or 0),
        min_purchase_count=int(o.get("min_purchase_count", getattr(t, "min_purchase_count", 0) or 0) or 0),
        min_purchase_amount=int(o.get("min_purchase_amount", getattr(t, "min_purchase_amount", 0) or 0) or 0),
        rank=int(getattr(t, "rank", 0) or 0),
    )


def preview_tier_threshold_impact(db: Session, *, brand: str, payload: PreviewImpactIn) -> dict:
    from app.services.loyalty_status_service import (
        _tier_meets_points,
        _tier_meets_purchases,
        _tier_mode_sort_key,
    )
    from app.services.purchase_service import get_purchase_balances

    mode = get_qualification_mode(db, brand=brand)
    existing = db.query(LoyaltyTier).filter(LoyaltyTier.brand == brand).all()
    simulated: list = []
    matched = False
    overrides = {
        k: v
        for k, v in {
            "key": payload.key,
            "name": payload.name,
            "min_status_points": payload.min_status_points,
            "min_purchase_count": payload.min_purchase_count,
            "min_purchase_amount": payload.min_purchase_amount,
            "active": payload.active,
        }.items()
        if v is not None
    }
    for t in existing:
        if payload.tier_id is not None and t.id == payload.tier_id:
            matched = True
            simulated.append(_simulated_tier_row(t, overrides=overrides))
        else:
            simulated.append(_simulated_tier_row(t))
    if payload.tier_id is None:
        from types import SimpleNamespace

        simulated.append(
            SimpleNamespace(
                key=payload.key or _slug_key(payload.name or "tier"),
                name=payload.name or "Nouveau",
                active=True if payload.active is None else bool(payload.active),
                min_status_points=int(payload.min_status_points or 0),
                min_purchase_count=int(payload.min_purchase_count or 0),
                min_purchase_amount=int(payload.min_purchase_amount or 0),
                rank=0,
            )
        )
    elif not matched:
        raise HTTPException(status_code=404, detail="Tier not found")

    active_rows = [t for t in simulated if bool(t.active)]
    errors = []
    if mode == QUALIFICATION_PURCHASES:
        errors = purchase_threshold_errors(
            [
                {
                    "name": t.name,
                    "key": t.key,
                    "count": int(t.min_purchase_count or 0),
                    "amount": int(t.min_purchase_amount or 0),
                }
                for t in active_rows
            ]
        )
    ordered = sorted(active_rows, key=lambda t: _tier_mode_sort_key(t, mode))
    rank_by_key = {t.key: i for i, t in enumerate(ordered)}

    def earned_key(status_points, purchase_count, purchase_amount):
        if not ordered:
            return None
        if mode == QUALIFICATION_PURCHASES:
            qualifying = [t for t in ordered if _tier_meets_purchases(t, purchase_count, purchase_amount)]
        else:
            qualifying = [t for t in ordered if _tier_meets_points(t, status_points)]
        if qualifying:
            return max(qualifying, key=lambda t: _tier_mode_sort_key(t, mode)).key
        return ordered[0].key

    customers = db.query(Customer).filter(Customer.brand == brand).all()
    will_rise = will_fall = unchanged = 0
    samples: list[dict] = []
    if not errors:
        for c in customers:
            count, amount = get_purchase_balances(db, c.id)
            earned = earned_key(int(c.status_points or 0), count, amount)
            old_rank = rank_by_key.get(c.loyalty_status)
            new_rank = rank_by_key.get(earned) if earned else None
            if old_rank is None and new_rank is None:
                unchanged += 1
                movement = "unchanged"
            elif new_rank is not None and (old_rank is None or new_rank > old_rank):
                will_rise += 1
                movement = "up"
            elif old_rank is not None and (new_rank is None or new_rank < old_rank):
                will_fall += 1
                movement = "down"
            else:
                unchanged += 1
                movement = "unchanged"
            if len(samples) < 20 and movement != "unchanged":
                samples.append(
                    {
                        "profileId": c.profile_id,
                        "fromTier": c.loyalty_status,
                        "toTier": earned,
                        "movement": movement,
                    }
                )
    return {
        "willRise": will_rise,
        "willFallAtExpiry": will_fall,
        "unchanged": unchanged,
        "samples": samples,
        "errors": errors,
        "ready": not errors,
    }


def _validate_tier_payload(
    *,
    db: Session,
    brand: str,
    tier_id: UUID | None,
    key: str,
    name: str,
    rank: int,
    min_status_points: int,
    min_purchase_count: int = 0,
    min_purchase_amount: int = 0,
    active: bool,
):
    if not (name and str(name).strip()):
        raise HTTPException(status_code=400, detail="Level name is required")

    min_i = _as_non_negative_int(min_status_points, field="min_status_points")
    min_count = _as_non_negative_int(min_purchase_count, field="min_purchase_count")
    min_amount = _as_non_negative_int(min_purchase_amount, field="min_purchase_amount")

    dup_name_q = db.query(LoyaltyTier.id).filter(LoyaltyTier.brand == brand).filter(LoyaltyTier.name == name)
    if tier_id is not None:
        dup_name_q = dup_name_q.filter(LoyaltyTier.id != tier_id)
    if dup_name_q.first():
        raise HTTPException(status_code=400, detail="A level with this name already exists for this brand")

    mode = get_qualification_mode(db, brand=brand)

    tiers = db.query(LoyaltyTier).filter(LoyaltyTier.brand == brand).all()
    simulated = []
    for t in tiers:
        if tier_id is not None and t.id == tier_id:
            simulated.append(
                {
                    "min": min_i,
                    "count": min_count,
                    "amount": min_amount,
                    "key": key,
                    "name": name,
                    "active": bool(active),
                }
            )
        else:
            simulated.append(
                {
                    "min": int(t.min_status_points),
                    "count": int(getattr(t, "min_purchase_count", 0) or 0),
                    "amount": int(getattr(t, "min_purchase_amount", 0) or 0),
                    "key": t.key,
                    "name": t.name,
                    "active": bool(t.active),
                }
            )
    if tier_id is None:
        simulated.append(
            {
                "min": min_i,
                "count": min_count,
                "amount": min_amount,
                "key": key,
                "name": name,
                "active": bool(active),
            }
        )
    active_rows = [r for r in simulated if r["active"]]

    if mode == QUALIFICATION_PURCHASES:
        errors = purchase_threshold_errors(active_rows)
        if errors:
            raise HTTPException(status_code=400, detail=" ".join(errors))
        has_base = any(int(t["count"]) == 0 and int(t["amount"]) == 0 for t in active_rows)
        if not has_base:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Invalid levels: you must have at least one active base level "
                    "with min_purchase_count=0 and min_purchase_amount=0"
                ),
            )
    else:
        ordered_points = sorted(active_rows, key=lambda x: x["min"])
        for prev, cur in zip(ordered_points, ordered_points[1:]):
            if cur["min"] <= prev["min"]:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Invalid levels: minimum status points must be unique and strictly increasing"
                    ),
                )
        has_base = any(int(t["min"]) == 0 for t in active_rows)
        if not has_base:
            raise HTTPException(
                status_code=400,
                detail="Invalid levels: you must have at least one active base level with min_status_points=0",
            )


def target_mode_threshold_errors(db: Session, brand: str, target_mode: str) -> list[str]:
    """Refuse a mode switch until that mode's thresholds are real (not placeholders)."""
    from app.services.purchase_service import QUALIFICATION_PURCHASES, normalize_qualification_mode

    mode = normalize_qualification_mode(target_mode)
    tiers = (
        db.query(LoyaltyTier)
        .filter(LoyaltyTier.brand == brand)
        .filter(LoyaltyTier.active.is_(True))
        .all()
    )
    errors: list[str] = []
    if not tiers:
        return ["No active loyalty tiers"]
    if mode == QUALIFICATION_PURCHASES:
        rows = [
            {
                "name": t.name,
                "key": t.key,
                "count": int(getattr(t, "min_purchase_count", 0) or 0),
                "amount": int(getattr(t, "min_purchase_amount", 0) or 0),
            }
            for t in tiers
        ]
        ordered = sorted(rows, key=lambda r: (r["count"], r["amount"]))
        base = ordered[0]
        if int(base["count"]) != 0 or int(base["amount"]) != 0:
            errors.append("PURCHASES needs an active base tier with min_purchase_count=0 and min_purchase_amount=0")
        errors.extend(purchase_threshold_errors(rows))
        for row in ordered[1:]:
            if int(row["count"]) < 1 or int(row["amount"]) < 1:
                errors.append(
                    f"Tier {row['key']}: set real min_purchase_count (>=1) and min_purchase_amount (>=1) before switching"
                )
    else:
        ordered = sorted(tiers, key=lambda t: int(t.min_status_points or 0))
        if int(ordered[0].min_status_points or 0) != 0:
            errors.append("POINTS needs an active base tier with min_status_points=0")
        for prev, cur in zip(ordered, ordered[1:]):
            if int(cur.min_status_points or 0) <= int(prev.min_status_points or 0):
                errors.append(
                    f"POINTS thresholds must strictly increase ({prev.key} → {cur.key})"
                )
    return errors


def preview_qualification_mode_switch(db: Session, *, brand: str, target_mode: str) -> dict:
    from app.services.loyalty_status_service import (
        _get_tier_rank_value,
        compute_loyalty_status_from_tiers,
    )
    from app.services.purchase_service import QUALIFICATION_PURCHASES, get_purchase_balances, normalize_qualification_mode

    mode = normalize_qualification_mode(target_mode)
    errors = target_mode_threshold_errors(db, brand, mode)
    customers = db.query(Customer).filter(Customer.brand == brand).all()
    up = down = unchanged = 0
    samples: list[dict] = []
    if not errors:
        for c in customers:
            count, amount = get_purchase_balances(db, c.id) if mode == QUALIFICATION_PURCHASES else (
                int(c.purchase_count or 0),
                int(c.purchase_amount_total or 0),
            )
            earned = compute_loyalty_status_from_tiers(
                db,
                brand,
                int(c.status_points or 0),
                purchase_count=count,
                purchase_amount=amount,
                qualification_mode=mode,
            )
            old_rank = _get_tier_rank_value(db, brand, c.loyalty_status) if c.loyalty_status else None
            new_rank = _get_tier_rank_value(db, brand, earned) if earned else None
            if old_rank is None and new_rank is None:
                unchanged += 1
                movement = "unchanged"
            elif new_rank is not None and (old_rank is None or new_rank > old_rank):
                up += 1
                movement = "up"
            elif old_rank is not None and (new_rank is None or new_rank < old_rank):
                down += 1
                movement = "down"
            else:
                unchanged += 1
                movement = "unchanged"
            if len(samples) < 20 and movement != "unchanged":
                samples.append(
                    {
                        "profileId": c.profile_id,
                        "fromTier": c.loyalty_status,
                        "toTier": earned,
                        "movement": movement,
                    }
                )
    return {
        "targetMode": mode,
        "ready": not errors,
        "errors": errors,
        "customers": len(customers),
        "up": up,
        "down": down,
        "unchanged": unchanged,
        "samples": samples,
    }


@router.get("", response_model=list[LoyaltyTierOut])
def list_loyalty_tiers(
    active_brand: str = Depends(get_active_brand),
    brand: str | None = None,
    active: bool | None = None,
    db: Session = Depends(get_db),
):
    q = db.query(LoyaltyTier)
    if brand and brand != active_brand:
        raise HTTPException(status_code=400, detail="brand does not match active brand context")
    q = q.filter(LoyaltyTier.brand == active_brand)
    if active is not None:
        q = q.filter(LoyaltyTier.active.is_(active))
    return q.order_by(LoyaltyTier.brand.asc(), LoyaltyTier.rank.asc(), LoyaltyTier.created_at.asc()).all()


@router.post("", response_model=LoyaltyTierOut)
def create_loyalty_tier(
    payload: LoyaltyTierCreate,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    if payload.brand is not None and payload.brand != active_brand:
        raise HTTPException(status_code=400, detail="payload.brand does not match active brand context")
    key_in = (payload.key or "").strip() or None
    key = key_in or _slug_key(payload.name)
    base = key
    i = 1
    while (
        db.query(LoyaltyTier.id)
        .filter(LoyaltyTier.brand == active_brand)
        .filter(LoyaltyTier.key == key)
        .first()
    ):
        i += 1
        key = f"{base}_{i}"

    min_purchase_count = int(getattr(payload, "min_purchase_count", 0) or 0)
    min_purchase_amount = int(getattr(payload, "min_purchase_amount", 0) or 0)
    min_status_points = int(getattr(payload, "min_status_points", 0) or 0)
    if get_qualification_mode(db, brand=active_brand) == QUALIFICATION_PURCHASES:
        min_status_points = unique_min_status_points(
            _used_min_status_points(db, active_brand),
            min_status_points,
        )
    _validate_tier_payload(
        db=db,
        brand=active_brand,
        tier_id=None,
        key=key,
        name=payload.name,
        rank=0,
        min_status_points=min_status_points,
        min_purchase_count=min_purchase_count,
        min_purchase_amount=min_purchase_amount,
        active=payload.active,
    )

    obj = LoyaltyTier(
        brand=active_brand,
        key=key,
        name=payload.name,
        min_status_points=min_status_points,
        min_purchase_count=min_purchase_count,
        min_purchase_amount=min_purchase_amount,
        rank=_initial_rank_for_new_tier(
            db=db,
            brand=active_brand,
            min_status_points=min_status_points,
        ),
        active=payload.active,
    )
    db.add(obj)
    db.flush()
    adjustments = _align_inactive_mode_thresholds(db, active_brand)
    db.commit()
    db.refresh(obj)

    _enqueue_recompute_customers_job(db, active_brand)

    return _tier_out(obj, adjustments)


@router.get("/consistency")
def loyalty_tiers_consistency(
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    return diagnose_tier_consistency(db, active_brand)


@router.post("/preview-impact")
def preview_tier_impact(
    payload: PreviewImpactIn,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    return preview_tier_threshold_impact(db, brand=active_brand, payload=payload)


@router.get("/{tier_id}", response_model=LoyaltyTierOut)
def get_loyalty_tier(
    tier_id: UUID,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    obj = db.query(LoyaltyTier).filter(LoyaltyTier.id == tier_id).first()
    if not obj or obj.brand != active_brand:
        raise HTTPException(status_code=404, detail="Tier not found")
    return obj


@router.patch("/{tier_id}", response_model=LoyaltyTierOut)
def update_loyalty_tier(
    tier_id: UUID,
    payload: LoyaltyTierUpdate,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    obj = db.query(LoyaltyTier).filter(LoyaltyTier.id == tier_id).first()
    if not obj or obj.brand != active_brand:
        raise HTTPException(status_code=404, detail="Tier not found")

    data = payload.model_dump(exclude_unset=True)
    if "brand" in data and data["brand"] is not None and data["brand"] != active_brand:
        raise HTTPException(status_code=400, detail="payload.brand does not match active brand context")

    if "key" in data and data["key"] != obj.key:
        existing = (
            db.query(LoyaltyTier.id)
            .filter(LoyaltyTier.brand == obj.brand)
            .filter(LoyaltyTier.key == data["key"])
            .first()
        )
        if existing:
            raise HTTPException(status_code=400, detail="Tier key already exists for brand")

    final_key = data.get("key", obj.key)
    final_name = data.get("name", obj.name)
    final_min = data.get("min_status_points", obj.min_status_points)
    final_count = data.get("min_purchase_count", getattr(obj, "min_purchase_count", 0) or 0)
    final_amount = data.get("min_purchase_amount", getattr(obj, "min_purchase_amount", 0) or 0)
    final_active = data.get("active", obj.active)
    if get_qualification_mode(db, brand=active_brand) == QUALIFICATION_PURCHASES:
        final_min = unique_min_status_points(
            _used_min_status_points(db, active_brand, exclude_id=tier_id),
            int(final_min or 0),
        )
        data["min_status_points"] = final_min
    _validate_tier_payload(
        db=db,
        brand=active_brand,
        tier_id=tier_id,
        key=final_key,
        name=final_name,
        rank=0,
        min_status_points=final_min,
        min_purchase_count=final_count,
        min_purchase_amount=final_amount,
        active=final_active,
    )

    for k, v in data.items():
        if k == "brand":
            continue
        if k == "rank":
            continue
        setattr(obj, k, v)

    db.flush()
    adjustments = _align_inactive_mode_thresholds(db, active_brand)
    db.commit()
    db.refresh(obj)

    _enqueue_recompute_customers_job(db, active_brand)

    return _tier_out(obj, adjustments)


@router.delete("/{tier_id}")
def delete_loyalty_tier(
    tier_id: UUID,
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    obj = db.query(LoyaltyTier).filter(LoyaltyTier.id == tier_id).first()
    if not obj or obj.brand != active_brand:
        raise HTTPException(status_code=404, detail="Tier not found")

    is_base = (
        int(obj.min_status_points) == 0
        and int(getattr(obj, "min_purchase_count", 0) or 0) == 0
        and int(getattr(obj, "min_purchase_amount", 0) or 0) == 0
    )
    if bool(obj.active) and is_base:
        remaining_active_zero = (
            db.query(func.count(LoyaltyTier.id))
            .filter(LoyaltyTier.brand == active_brand)
            .filter(LoyaltyTier.active.is_(True))
            .filter(LoyaltyTier.min_status_points == 0)
            .filter(LoyaltyTier.min_purchase_count == 0)
            .filter(LoyaltyTier.min_purchase_amount == 0)
            .filter(LoyaltyTier.id != obj.id)
            .scalar()
            or 0
        )
        if int(remaining_active_zero) <= 0:
            raise HTTPException(
                status_code=400,
                detail=(
                    "You cannot delete the last active base level "
                    "(min_status_points=0, min_purchase_count=0, min_purchase_amount=0)"
                ),
            )

    db.delete(obj)
    db.flush()
    _align_inactive_mode_thresholds(db, active_brand)
    db.commit()

    _enqueue_recompute_customers_job(db, active_brand)

    return {"deleted": True}


@router.post("/recompute-customers")
def recompute_customers_loyalty_status(
    payload: RecomputeCustomersIn = Body(default_factory=RecomputeCustomersIn),
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    apply_downgrades = bool(payload and payload.applyDowngrades)
    _enqueue_recompute_customers_job(db, active_brand, apply_downgrades=apply_downgrades)
    return {"enqueued": True, "brand": active_brand, "applyDowngrades": apply_downgrades}


@router.post("/ensure-base-tier", response_model=LoyaltyTierOut)
def ensure_base_loyalty_tier(
    active_brand: str = Depends(get_active_brand),
    db: Session = Depends(get_db),
):
    existing = (
        db.query(LoyaltyTier)
        .filter(LoyaltyTier.brand == active_brand)
        .filter(LoyaltyTier.min_status_points == 0)
        .filter(LoyaltyTier.min_purchase_count == 0)
        .filter(LoyaltyTier.min_purchase_amount == 0)
        .first()
    )
    if not existing:
        existing = (
            db.query(LoyaltyTier)
            .filter(LoyaltyTier.brand == active_brand)
            .filter(LoyaltyTier.min_status_points == 0)
            .order_by(LoyaltyTier.rank.asc())
            .first()
        )
    if existing:
        if not bool(existing.active):
            _validate_tier_payload(
                db=db,
                brand=active_brand,
                tier_id=existing.id,
                key=existing.key,
                name=existing.name,
                rank=0,
                min_status_points=0,
                active=True,
            )
            existing.active = True
            db.flush()
            _align_inactive_mode_thresholds(db, active_brand)
            db.commit()
            db.refresh(existing)

            _enqueue_recompute_customers_job(db, active_brand)

        return existing

    name = "Base"
    key = _slug_key(name)
    base = key
    i = 1
    while (
        db.query(LoyaltyTier.id)
        .filter(LoyaltyTier.brand == active_brand)
        .filter(LoyaltyTier.key == key)
        .first()
    ):
        i += 1
        key = f"{base}_{i}"

    _validate_tier_payload(
        db=db,
        brand=active_brand,
        tier_id=None,
        key=key,
        name=name,
        rank=0,
        min_status_points=0,
        active=True,
    )

    obj = LoyaltyTier(
        brand=active_brand,
        key=key,
        name=name,
        min_status_points=0,
        min_purchase_count=0,
        min_purchase_amount=0,
        rank=0,
        active=True,
    )
    db.add(obj)
    db.flush()
    _align_inactive_mode_thresholds(db, active_brand)
    db.commit()
    db.refresh(obj)

    _enqueue_recompute_customers_job(db, active_brand)

    return obj
