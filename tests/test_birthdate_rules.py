"""Rule engine birthdate granularity."""

from types import SimpleNamespace

from app.services.rule_engine import _evaluate_ast_condition


def _customer(**kwargs):
    return SimpleNamespace(brand="batira", **kwargs)


class _Tx:
    payload = {}


def test_rule_month_target():
    customer = _customer(birth_month=6, birth_day=1)
    node = {"field": "customer.birthdate", "operator": "eq", "value": "06"}
    assert _evaluate_ast_condition(db=None, customer=customer, transaction=_Tx(), node=node) is True


def test_rule_year_target():
    customer = _customer(birth_year=1990, birth_month=3, birth_day=1)
    node = {"field": "customer.birthdate", "operator": "eq", "value": "1990"}
    assert _evaluate_ast_condition(db=None, customer=customer, transaction=_Tx(), node=node) is True


def test_rule_system_today_evaluates():
    customer = _customer(birth_month=6, birth_day=12, birth_year=1985)
    node = {"field": "customer.birthdate", "operator": "eq", "value": {"$system": "today"}}
    result = _evaluate_ast_condition(db=None, customer=customer, transaction=_Tx(), node=node)
    assert isinstance(result, bool)
