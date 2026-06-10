"""Bidirectional coupon_type_rewards link tests (service layer)."""

from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from app.services.coupon_rewards_service import (
    link_reward_to_coupon_types,
    replace_coupon_type_rewards,
    replace_reward_coupon_types,
)


def _mock_db_with_delete():
    db = MagicMock()
    query = MagicMock()
    query.filter.return_value = query
    query.delete.return_value = 0
    query.first.return_value = None
    db.query.return_value = query
    db.flush = MagicMock()
    db.add = MagicMock()
    return db, query


def test_replace_coupon_type_rewards_clears_then_adds():
    db, query = _mock_db_with_delete()
    coupon_type = SimpleNamespace(id=uuid4(), brand="batira")
    reward_id = str(uuid4())

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "app.services.coupon_rewards_service._load_rewards",
            lambda *_a, **_k: {reward_id: SimpleNamespace(id=reward_id)},
        )
        out = replace_coupon_type_rewards(
            db,
            coupon_type=coupon_type,
            reward_ids=[reward_id],
            brand="batira",
        )
    assert out == [reward_id]
    query.delete.assert_called()
    db.add.assert_called()


def test_replace_reward_coupon_types_uses_replace_mode():
    db, query = _mock_db_with_delete()
    reward = SimpleNamespace(id=uuid4(), brand="batira")
    coupon_type_id = str(uuid4())

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "app.services.coupon_rewards_service._load_coupon_types",
            lambda *_a, **_k: {coupon_type_id: SimpleNamespace(id=coupon_type_id)},
        )
        mp.setattr(
            "app.services.coupon_rewards_service.list_reward_coupon_type_ids",
            lambda *_a, **_k: [coupon_type_id],
        )
        out = replace_reward_coupon_types(
            db,
            reward=reward,
            coupon_type_ids=[coupon_type_id],
            brand="batira",
        )
    assert str(out[0]) == coupon_type_id
    assert query.delete.call_count >= 1


def test_link_reward_empty_list_clears_all_when_replace():
    db, query = _mock_db_with_delete()
    reward = SimpleNamespace(id=uuid4(), brand="batira")

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(
            "app.services.coupon_rewards_service.list_reward_coupon_type_ids",
            lambda *_a, **_k: [],
        )
        out = link_reward_to_coupon_types(
            db,
            reward=reward,
            coupon_type_ids=[],
            brand="batira",
            replace=True,
        )
    assert out == []
    query.delete.assert_called()
