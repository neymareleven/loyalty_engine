"""Birthdate targeting — parsing, comparison, Unomi translation."""

from datetime import date
from types import SimpleNamespace

import pytest

from app.services.birthdate_targeting import (
    BirthdateGranularity,
    compare_birthdate,
    format_customer_birthdate_wire,
    parse_birthdate_wire,
    parse_customer_birthdate_storage,
)
from app.services.segment_condition_unomi import (
    loyalty_ast_to_unomi_condition,
    unomi_condition_to_loyalty_ast,
)


def _customer(**kwargs):
    return SimpleNamespace(**kwargs)


def test_parse_full_date():
    t = parse_birthdate_wire("1990-06-12")
    assert t.granularity == BirthdateGranularity.FULL
    assert t.wire == "1990-06-12"


def test_parse_day_month():
    t = parse_birthdate_wire("06-12")
    assert t.granularity == BirthdateGranularity.DAY_MONTH
    assert t.mmdd == 612


def test_parse_day_month_flex():
    t = parse_birthdate_wire("6-10")
    assert t.wire == "06-10"


def test_parse_month_only():
    t = parse_birthdate_wire("06")
    assert t.granularity == BirthdateGranularity.MONTH
    assert t.month == 6


def test_parse_month_dash_form():
    t = parse_birthdate_wire("--06")
    assert t.granularity == BirthdateGranularity.MONTH
    assert t.wire == "06"


def test_parse_year_only():
    t = parse_birthdate_wire("1990")
    assert t.granularity == BirthdateGranularity.YEAR
    assert t.year == 1990


def test_reject_slash_format():
    with pytest.raises(ValueError, match="DD/MM/YYYY"):
        parse_birthdate_wire("12/06/1990")


def test_compare_anniversary():
    customer = _customer(birth_month=6, birth_day=12, birth_year=1990)
    assert compare_birthdate(op="eq", customer=customer, expected="06-12") is True
    assert compare_birthdate(op="eq", customer=customer, expected="06-13") is False


def test_compare_month_only():
    customer = _customer(birth_month=6, birth_day=12, birth_year=1990)
    assert compare_birthdate(op="eq", customer=customer, expected="06") is True
    assert compare_birthdate(op="eq", customer=customer, expected="07") is False


def test_compare_year_only():
    customer = _customer(birth_month=6, birth_day=12, birth_year=1990)
    assert compare_birthdate(op="eq", customer=customer, expected="1990") is True
    assert compare_birthdate(op="eq", customer=customer, expected="1991") is False


def test_compare_full_date():
    customer = _customer(birth_month=6, birth_day=12, birth_year=1990, birthdate=date(1990, 6, 12))
    assert compare_birthdate(op="eq", customer=customer, expected="1990-06-12") is True


def test_format_customer_wire_all_granularities():
    assert format_customer_birthdate_wire(_customer(birth_year=1990, birth_month=6, birth_day=12)) == "1990-06-12"
    assert format_customer_birthdate_wire(_customer(birth_month=6, birth_day=12)) == "06-12"
    assert format_customer_birthdate_wire(_customer(birth_month=6)) == "06"
    assert format_customer_birthdate_wire(_customer(birth_year=1990)) == "1990"


def test_parse_customer_storage_month_only():
    full, mm, dd, yy = parse_customer_birthdate_storage("06")
    assert full is None and mm == 6 and dd is None and yy is None


def test_parse_customer_storage_year_only():
    full, mm, dd, yy = parse_customer_birthdate_storage("1990")
    assert full is None and mm is None and dd is None and yy == 1990


def test_unomi_month_target():
    cond = loyalty_ast_to_unomi_condition(
        {"field": "customer.birthdate", "operator": "eq", "value": "06"}
    )
    params = cond["parameterValues"]
    assert params["propertyName"] == "properties.birthMonth"
    assert params["propertyValueInteger"] == 6


def test_unomi_year_target():
    cond = loyalty_ast_to_unomi_condition(
        {"field": "customer.birthdate", "operator": "eq", "value": "1990"}
    )
    params = cond["parameterValues"]
    assert params["propertyName"] == "properties.birthYear"
    assert params["propertyValueInteger"] == 1990


def test_unomi_system_today_is_day_month():
    cond = loyalty_ast_to_unomi_condition(
        {"field": "customer.birthdate", "operator": "eq", "value": {"$system": "today"}}
    )
    params = cond["parameterValues"]
    assert params["propertyName"] == "properties.birthDate"
    assert "propertyValue" in params
    assert len(params["propertyValue"]) == 5


def test_unomi_month_roundtrip():
    cond = loyalty_ast_to_unomi_condition(
        {"field": "customer.birthdate", "operator": "eq", "value": "06"}
    )
    ast = unomi_condition_to_loyalty_ast(cond)
    assert ast == {"field": "customer.birthdate", "operator": "eq", "value": "06"}
