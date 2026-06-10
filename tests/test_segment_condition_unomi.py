"""Unomi condition translation — typed propertyValue* keys and profile-sync alignment."""

from app.services.segment_condition_unomi import (
    loyalty_ast_to_unomi_condition,
    unomi_condition_to_loyalty_ast,
)


def test_status_points_uses_property_value_integer():
    cond = loyalty_ast_to_unomi_condition(
        {"field": "customer.status_points", "operator": "gte", "value": 100}
    )
    params = cond["parameterValues"]
    assert params["propertyName"] == "properties.statusPoints"
    assert params["propertyValueInteger"] == 100
    assert "propertyValue" not in params


def test_metrics_count_uses_property_value_integer():
    cond = loyalty_ast_to_unomi_condition(
        {
            "field": "customer.metrics.transactions_count_30d",
            "operator": "gte",
            "value": 2,
        }
    )
    params = cond["parameterValues"]
    assert params["propertyName"] == "properties.metrics.transactions_count_30d"
    assert params["propertyValueInteger"] == 2


def test_loyalty_status_uses_property_value_string():
    cond = loyalty_ast_to_unomi_condition(
        {"field": "customer.loyalty_status", "operator": "eq", "value": "gold"}
    )
    params = cond["parameterValues"]
    assert params["propertyValue"] == "gold"
    assert "propertyValueInteger" not in params


def test_birthdate_full_date_uses_epoch_ms_integer():
    cond = loyalty_ast_to_unomi_condition(
        {"field": "customer.birthdate", "operator": "eq", "value": "1990-05-15"}
    )
    params = cond["parameterValues"]
    assert params["propertyName"] == "properties.birthDate"
    assert "propertyValueInteger" in params
    assert params["propertyValueInteger"] > 0
    assert "propertyValueDate" not in params


def test_birthday_alias_maps_to_birthdate():
    cond = loyalty_ast_to_unomi_condition(
        {"field": "customer.birthday", "operator": "eq", "value": "05-15"}
    )
    params = cond["parameterValues"]
    assert params["propertyName"] == "properties.birthDate"
    assert params["propertyValue"] == "05-15"


def test_created_at_maps_to_loyalty_created_at_property():
    cond = loyalty_ast_to_unomi_condition(
        {
            "field": "customer.created_at",
            "operator": "gte",
            "value": "2024-01-01T00:00:00",
        }
    )
    params = cond["parameterValues"]
    assert params["propertyName"] == "properties.loyaltyCreatedAt"
    assert params["propertyValueDate"] == "2024-01-01T00:00:00"


def test_last_activity_at_maps_to_last_activity_at_property():
    cond = loyalty_ast_to_unomi_condition(
        {
            "field": "customer.last_activity_at",
            "operator": "gte",
            "value": "2024-06-01T12:00:00",
        }
    )
    params = cond["parameterValues"]
    assert params["propertyName"] == "properties.lastActivityAt"
    assert params["propertyValueDate"] == "2024-06-01T12:00:00"


def test_unomi_birthdate_integer_roundtrip_to_iso_date():
    cond = loyalty_ast_to_unomi_condition(
        {"field": "customer.birthdate", "operator": "eq", "value": "1990-05-15"}
    )
    ast = unomi_condition_to_loyalty_ast(cond)
    assert ast == {
        "field": "customer.birthdate",
        "operator": "eq",
        "value": "1990-05-15",
    }
