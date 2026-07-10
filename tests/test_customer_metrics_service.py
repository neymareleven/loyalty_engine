"""Customer metrics include transactions ingested under alias profileIds."""

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import uuid4

from app.services.contact_service import build_brand_profile_id_to_customer_map
from app.services.customer_metrics_service import _merge_profile_aggregates_by_customer


def test_merge_profile_aggregates_sums_counts_and_max_last_at():
    customer_id = uuid4()
    older = datetime(2026, 6, 18, 10, 0, 0)
    newer = datetime(2026, 7, 9, 15, 55, 0)

    merged = _merge_profile_aggregates_by_customer(
        profile_to_customer={
            "master-id": customer_id,
            "alias-id": customer_id,
        },
        aggregates_by_profile={
            "master-id": {
                "last_transaction_at": older,
                "count_30d": 1,
                "count_90d": 2,
            },
            "alias-id": {
                "last_transaction_at": newer,
                "count_30d": 1,
                "count_90d": 1,
            },
        },
    )

    assert merged[customer_id]["count_30d"] == 2
    assert merged[customer_id]["count_90d"] == 3
    assert merged[customer_id]["last_transaction_at"] == newer


def test_build_brand_profile_id_to_customer_map_includes_aliases():
    db = MagicMock()
    customer_id = uuid4()
    master = "6b8555e2-master"
    alias = "87a759c2-alias"

    customer_query = MagicMock()
    customer_query.filter.return_value = customer_query
    customer_query.all.return_value = [SimpleNamespace(id=customer_id, profile_id=master)]

    alias_query = MagicMock()
    alias_query.filter.return_value = alias_query
    alias_query.all.return_value = [(alias, customer_id)]

    db.query.side_effect = [customer_query, alias_query]

    mapping = build_brand_profile_id_to_customer_map(db, brand="batira")

    assert mapping[master] == customer_id
    assert mapping[alias] == customer_id
