from datetime import datetime, timedelta

from sqlalchemy import and_, case, func
from sqlalchemy.orm import Session
from uuid import UUID

from app.models.customer import Customer
from app.models.customer_metrics import CustomerMetrics
from app.models.transaction import Transaction
from app.services.contact_service import build_brand_profile_id_to_customer_map


def _merge_profile_aggregates_by_customer(
    *,
    profile_to_customer: dict[str, UUID],
    aggregates_by_profile: dict[str, dict],
) -> dict[UUID, dict]:
    """Sum counts and take max(last_transaction_at) across master + alias profileIds."""
    merged: dict[UUID, dict] = {}
    for profile_id, agg in aggregates_by_profile.items():
        customer_id = profile_to_customer.get(profile_id)
        if customer_id is None:
            continue

        current = merged.get(customer_id)
        if current is None:
            merged[customer_id] = {
                "last_transaction_at": agg["last_transaction_at"],
                "count_30d": int(agg["count_30d"]),
                "count_90d": int(agg["count_90d"]),
            }
            continue

        current["count_30d"] += int(agg["count_30d"])
        current["count_90d"] += int(agg["count_90d"])
        last_at = agg["last_transaction_at"]
        if last_at and (
            current["last_transaction_at"] is None or last_at > current["last_transaction_at"]
        ):
            current["last_transaction_at"] = last_at

    return merged


def recompute_customer_metrics_for_brand(
    db: Session,
    *,
    brand: str,
    customer_ids: list[UUID] | None = None,
    now_utc: datetime | None = None,
) -> int:
    if now_utc is None:
        now_utc = datetime.utcnow()

    cutoff_30d = now_utc - timedelta(days=30)
    cutoff_90d = now_utc - timedelta(days=90)

    q_customers = db.query(Customer.id).filter(Customer.brand == brand)
    if customer_ids:
        q_customers = q_customers.filter(Customer.id.in_(customer_ids))

    customers = q_customers.all()
    if not customers:
        return 0

    customer_id_list = [row.id for row in customers]
    profile_to_customer = build_brand_profile_id_to_customer_map(
        db,
        brand=brand,
        customer_ids=customer_id_list,
    )
    profile_ids = list(profile_to_customer.keys())

    aggregates_by_profile: dict[str, dict] = {}
    if profile_ids:
        aggregates = (
            db.query(
                Transaction.profile_id.label("profile_id"),
                func.max(Transaction.created_at).label("last_transaction_at"),
                func.sum(case((Transaction.created_at >= cutoff_30d, 1), else_=0)).label("count_30d"),
                func.sum(case((Transaction.created_at >= cutoff_90d, 1), else_=0)).label("count_90d"),
            )
            .filter(and_(Transaction.brand == brand, Transaction.profile_id.in_(profile_ids)))
            .group_by(Transaction.profile_id)
            .all()
        )
        aggregates_by_profile = {
            a.profile_id: {
                "last_transaction_at": a.last_transaction_at,
                "count_30d": int(a.count_30d or 0),
                "count_90d": int(a.count_90d or 0),
            }
            for a in aggregates
        }

    agg_by_customer = _merge_profile_aggregates_by_customer(
        profile_to_customer=profile_to_customer,
        aggregates_by_profile=aggregates_by_profile,
    )

    existing = (
        db.query(CustomerMetrics)
        .filter(CustomerMetrics.brand == brand)
        .filter(CustomerMetrics.customer_id.in_(customer_id_list))
        .all()
    )
    existing_by_customer_id = {m.customer_id: m for m in existing}

    touched = 0
    for customer_id in customer_id_list:
        agg = agg_by_customer.get(customer_id)
        if agg is None:
            last_transaction_at = None
            count_30d = 0
            count_90d = 0
        else:
            last_transaction_at = agg["last_transaction_at"]
            count_30d = agg["count_30d"]
            count_90d = agg["count_90d"]

        m = existing_by_customer_id.get(customer_id)
        if m is None:
            m = CustomerMetrics(
                brand=brand,
                customer_id=customer_id,
                last_transaction_at=last_transaction_at,
                transactions_count_30d=count_30d,
                transactions_count_90d=count_90d,
                computed_at=now_utc,
            )
            db.add(m)
        else:
            m.last_transaction_at = last_transaction_at
            m.transactions_count_30d = count_30d
            m.transactions_count_90d = count_90d
            m.computed_at = now_utc
        touched += 1

    return touched
