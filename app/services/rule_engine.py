from datetime import datetime

import sqlalchemy as sa
from sqlalchemy.orm import Session
from sqlalchemy import asc
from app.models.customer import Customer
from app.models.loyalty_tier import LoyaltyTier
from app.models.rule import Rule
from app.models.transaction_rule_execution import TransactionRuleExecution
from app.models.point_movement import PointMovement
from app.models.customer_reward import CustomerReward
from app.services.contact_service import get_customer
from app.services.loyalty_service import earn_points, burn_points
from app.services.reward_service import issue_reward
from app.services.coupon_service import issue_coupon, use_coupon


def _get_by_path(obj, path: str):
    if obj is None:
        return None
    current = obj
    for part in path.split("."):
        if current is None:
            return None
        if isinstance(current, dict):
            current = current.get(part)
        else:
            current = getattr(current, part, None)
    return current


def _resolve_system_value(*, key: str, customer):
    now = datetime.utcnow()

    if key == "now":
        return now
    if key == "weekday":
        return now.weekday()

    if key == "customer_created_days":
        created_at = getattr(customer, "created_at", None)
        if not created_at:
            return None
        delta = now - created_at
        return int(delta.total_seconds() // 86400)

    if key == "customer_last_activity_days":
        last_activity_at = getattr(customer, "last_activity_at", None)
        if not last_activity_at:
            return None
        delta = now - last_activity_at
        return int(delta.total_seconds() // 86400)

    raise ValueError(f"Unknown system value preset: {key}")


def _resolve_expected_value(*, customer, value):
    if isinstance(value, dict) and "$system" in value:
        key = value.get("$system")
        if not isinstance(key, str) or not key:
            raise ValueError("Invalid system preset: expected non-empty '$system' string")
        return _resolve_system_value(key=key, customer=customer)

    if isinstance(value, list):
        return [_resolve_expected_value(customer=customer, value=v) for v in value]

    return value


def _as_int(value):
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def _as_float(value):
    try:
        if value is None:
            return None
        if isinstance(value, bool):
            return None
        return float(value)
    except Exception:
        return None


def _as_datetime(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except Exception:
            return None
    return None


def _as_mmdd(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return int(value.month) * 100 + int(value.day)
    if hasattr(value, "month") and hasattr(value, "day"):
        try:
            return int(value.month) * 100 + int(value.day)
        except Exception:
            return None
    if isinstance(value, str):
        s = value.strip()
        if len(s) == 5 and s[2] == "-":
            try:
                mm = int(s[0:2])
                dd = int(s[3:5])
            except Exception:
                return None
            if mm < 1 or mm > 12 or dd < 1 or dd > 31:
                return None
            return mm * 100 + dd
        if len(s) == 10 and s[4] == "-" and s[7] == "-":
            try:
                mm = int(s[5:7])
                dd = int(s[8:10])
            except Exception:
                return None
            if mm < 1 or mm > 12 or dd < 1 or dd > 31:
                return None
            return mm * 100 + dd
    return None


def _resolve_field_value(*, db: Session, field: str, customer, transaction):
    if not isinstance(field, str) or not field:
        raise ValueError("Condition leaf requires non-empty 'field'")

    if field.startswith("payload."):
        payload = transaction.payload or {}
        return _get_by_path(payload, field[len("payload.") :])

    if field.startswith("customer."):
        if field == "customer.rewards":
            if not getattr(customer, "id", None):
                return []
            rows = db.query(CustomerReward.reward_id).filter(CustomerReward.customer_id == customer.id).all()
            return [str(r[0]) for r in rows if r and r[0] is not None]
        if field == "customer.birthdate":
            yy = getattr(customer, "birth_year", None)
            mm = getattr(customer, "birth_month", None)
            dd = getattr(customer, "birth_day", None)
            if yy and mm and dd:
                return f"{int(yy):04d}-{int(mm):02d}-{int(dd):02d}"
            if mm and dd:
                return f"{int(mm):02d}-{int(dd):02d}"
            # Fallback to legacy column if present
            legacy = getattr(customer, "birthdate", None)
            if legacy is not None and hasattr(legacy, "month") and hasattr(legacy, "day") and hasattr(legacy, "year"):
                return f"{int(legacy.year):04d}-{int(legacy.month):02d}-{int(legacy.day):02d}"
            return None
        return _get_by_path(customer, field[len("customer.") :])

    if field.startswith("system."):
        key = field[len("system.") :]
        now = datetime.utcnow()

        if key == "now":
            return now
        if key == "weekday":
            return now.weekday()

        if key == "customer_created_days":
            created_at = getattr(customer, "created_at", None)
            if not created_at:
                return None
            delta = now - created_at
            return int(delta.total_seconds() // 86400)

        if key == "customer_last_activity_days":
            last_activity_at = getattr(customer, "last_activity_at", None)
            if not last_activity_at:
                return None
            delta = now - last_activity_at
            return int(delta.total_seconds() // 86400)

        raise ValueError(f"Unknown system field: {field}")

    raise ValueError(f"Unsupported field namespace: {field}. Use payload.*, customer.* or system.*")


def _op_exists(actual, expected):
    if expected is None:
        return actual not in (None, "")
    truthy = bool(expected)
    exists = actual not in (None, "")
    return exists if truthy else (not exists)


def _compare(*, op: str, actual, expected) -> bool:
    op = (op or "").lower()

    # Partial-birthdate-aware comparisons (month/day)
    a_mmdd = _as_mmdd(actual)
    e_mmdd = _as_mmdd(expected)
    if op in {"eq", "="} and (a_mmdd is not None or e_mmdd is not None):
        return a_mmdd is not None and e_mmdd is not None and a_mmdd == e_mmdd
    if op in {"neq", "!=", "ne"} and (a_mmdd is not None or e_mmdd is not None):
        return not (a_mmdd is not None and e_mmdd is not None and a_mmdd == e_mmdd)

    if op in {"eq", "="}:
        return actual == expected
    if op in {"neq", "!=", "ne"}:
        return actual != expected

    if op == "exists":
        return _op_exists(actual, expected)

    if op == "in":
        if not isinstance(expected, list):
            raise ValueError("Operator 'in' requires list value")
        if a_mmdd is not None and any(_as_mmdd(v) is not None for v in expected):
            exp_mmdd = {_as_mmdd(v) for v in expected}
            return a_mmdd in exp_mmdd
        if isinstance(actual, list):
            return any(a in expected for a in actual)
        return actual in expected

    if op == "contains":
        if isinstance(actual, list):
            return expected in actual
        if isinstance(actual, str):
            return str(expected) in actual
        return False

    if op == "between":
        if not isinstance(expected, list) or len(expected) != 2:
            raise ValueError("Operator 'between' requires [lo, hi]")

        if a_mmdd is not None and (_as_mmdd(expected[0]) is not None or _as_mmdd(expected[1]) is not None):
            lo = _as_mmdd(expected[0])
            hi = _as_mmdd(expected[1])
            if lo is None or hi is None:
                return False
            return lo <= a_mmdd <= hi

        a_dt = _as_datetime(actual)
        if a_dt is not None:
            lo = _as_datetime(expected[0])
            hi = _as_datetime(expected[1])
            if lo is None or hi is None:
                return False
            return lo <= a_dt <= hi

        a = _as_float(actual)
        lo = _as_float(expected[0])
        hi = _as_float(expected[1])
        if a is None or lo is None or hi is None:
            return False
        return lo <= a <= hi

    if op in {"gt", "gte", "lt", "lte"}:
        if a_mmdd is not None and e_mmdd is not None:
            if op == "gt":
                return a_mmdd > e_mmdd
            if op == "gte":
                return a_mmdd >= e_mmdd
            if op == "lt":
                return a_mmdd < e_mmdd
            if op == "lte":
                return a_mmdd <= e_mmdd

        a_dt = _as_datetime(actual)
        b_dt = _as_datetime(expected)
        if a_dt is not None and b_dt is not None:
            if op == "gt":
                return a_dt > b_dt
            if op == "gte":
                return a_dt >= b_dt
            if op == "lt":
                return a_dt < b_dt
            if op == "lte":
                return a_dt <= b_dt

        a = _as_float(actual)
        b = _as_float(expected)
        if a is None or b is None:
            return False
        if op == "gt":
            return a > b
        if op == "gte":
            return a >= b
        if op == "lt":
            return a < b
        if op == "lte":
            return a <= b

    raise ValueError(f"Unsupported operator: {op}")


def _evaluate_ast_condition(*, db: Session, customer, transaction, node) -> bool:
    if node is None:
        return True

    if not isinstance(node, dict):
        raise ValueError("Invalid condition format: expected object")

    if "and" in node:
        items = node.get("and")
        if not isinstance(items, list):
            raise ValueError("Invalid 'and' condition: expected list")
        return all(_evaluate_ast_condition(db=db, customer=customer, transaction=transaction, node=i) for i in items)

    if "or" in node:
        items = node.get("or")
        if not isinstance(items, list):
            raise ValueError("Invalid 'or' condition: expected list")
        return any(_evaluate_ast_condition(db=db, customer=customer, transaction=transaction, node=i) for i in items)

    if "not" in node:
        return not _evaluate_ast_condition(db=db, customer=customer, transaction=transaction, node=node.get("not"))

    if "field" in node:
        field = node.get("field")
        op = node.get("operator")
        if op is None:
            op = node.get("op")
        if not op:
            raise ValueError("Condition leaf requires 'operator' (or alias 'op')")
        value = _resolve_expected_value(customer=customer, value=node.get("value"))
        actual = _resolve_field_value(db=db, field=field, customer=customer, transaction=transaction)
        return _compare(op=op, actual=actual, expected=value)

    raise ValueError(
        "Invalid condition format: expected {'and':[...]}, {'or':[...]}, {'not':...} or leaf {'field':..., 'operator':..., 'value':...}"
    )


def _evaluate_condition_block(db: Session, customer, transaction, conditions) -> bool:
    return _evaluate_ast_condition(db=db, customer=customer, transaction=transaction, node=conditions)


def _execute_actions(db: Session, customer, transaction, actions):
    if actions is None:
        return []
    if isinstance(actions, dict):
        actions = [actions]
    if not isinstance(actions, list):
        raise ValueError("Invalid actions format")

    executed = []
    for action_index, action in enumerate(actions):
        if not isinstance(action, dict):
            raise ValueError("Invalid action")

        action_type = action.get("type")

        # Backward-compatibility: old rules may still exist in DB.
        # These actions are deprecated and must have no side effects.
        if action_type in {"burn_points", "issue_reward", "use_coupon", "set_rank"}:
            executed.append({"type": str(action_type), "ignored": True})
            continue

        if action_type == "earn_points":
            points = action.get("points")
            multiplier = action.get("multiplier")
            points_int = _as_int(points)
            mult_int = _as_int(multiplier)
            if mult_int is not None:
                points_int = (points_int or 0) * mult_int

            depth = _as_int(_get_by_path(transaction.payload or {}, "_ruleDepth")) or 0
            earn_points(
                db,
                customer,
                points=points_int,
                source_transaction_id=transaction.id,
                depth=depth,
            )
            executed.append({"type": action_type, "points": points_int, "multiplier": mult_int})

        elif action_type == "issue_coupon":
            coupon_type_id = action.get("coupon_type_id") or action.get("couponTypeId") or action.get("couponTypeID")
            if isinstance(coupon_type_id, dict):
                coupon_type_id = coupon_type_id.get("id") or coupon_type_id.get("couponTypeId") or coupon_type_id.get(
                    "coupon_type_id"
                )
            if coupon_type_id is not None:
                coupon_type_id = str(coupon_type_id)
            if not coupon_type_id:
                raise ValueError("issue_coupon requires coupon_type_id")

            frequency = action.get("frequency") or "ONCE_PER_CALENDAR_YEAR"
            frequency = str(frequency)

            rule_id = _get_by_path(transaction.payload or {}, "_ruleContext.rule_id")
            rule_execution_id = _get_by_path(transaction.payload or {}, "_ruleContext.rule_execution_id")

            idempotency_key = None
            if transaction.id and rule_id and rule_execution_id and coupon_type_id:
                idempotency_key = f"issue_coupon:{transaction.id}:{rule_id}:{rule_execution_id}:{action_index}:{coupon_type_id}"

            issue_coupon(
                db,
                customer=customer,
                transaction=transaction,
                coupon_type_id=coupon_type_id,
                frequency=frequency,  # validated in service
                rule_id=str(rule_id) if rule_id is not None else None,
                rule_execution_id=str(rule_execution_id) if rule_execution_id is not None else None,
                idempotency_key=idempotency_key,
            )
            executed.append({"type": action_type, "couponTypeId": coupon_type_id, "frequency": frequency})

        elif action_type == "reset_status_points":
            locked_customer = db.query(Customer).filter(Customer.id == customer.id).with_for_update().one()

            from app.models.point_movement import PointMovement
            from app.services.wallet_service import get_status_points_balance

            balance = int(get_status_points_balance(db, locked_customer.id) or 0)
            if balance > 0:
                db.add(
                    PointMovement(
                        customer_id=locked_customer.id,
                        points=-balance,
                        type="ADJUST",
                        source_transaction_id=transaction.id,
                        expires_at=None,
                    )
                )

            locked_customer.status_points = 0
            locked_customer.status_points_reset_at = datetime.utcnow()
            db.flush()

            from app.services.loyalty_status_service import update_customer_status

            depth = _as_int(_get_by_path(transaction.payload or {}, "_ruleDepth")) or 0
            update_customer_status(
                db,
                locked_customer,
                reason="RESET",
                source_transaction_id=transaction.id,
                depth=depth,
            )
            executed.append({"type": action_type})

        else:
            raise ValueError(f"Unknown action type: {action_type}")

    return executed


def process_transaction_rules(db: Session, transaction):
    """
    Exécute les règles applicables à une transaction PENDING
    """

    # 🔹 Option 1 (strict): customer must already exist.
    customer = get_customer(db, transaction.brand, transaction.profile_id)
    if not customer:
        raise ValueError("Customer not found. Use /customers/upsert before sending business events.")

    depth = _as_int(_get_by_path(transaction.payload or {}, "_ruleDepth")) or 0
    if depth >= 3:
        transaction.status = "PROCESSED"
        return

    # Match rules by ANY (OR): transaction.transaction_type must be included in rule.transaction_types.
    # Backward compatibility: legacy rules may have transaction_types NULL; they match via transaction_type.
    rules = (
        db.query(Rule)
        .filter(Rule.brand == transaction.brand)
        .filter(Rule.active == True)
        .filter(
            sa.or_(
                Rule.transaction_types.any(transaction.transaction_type),
                sa.and_(Rule.transaction_types.is_(None), Rule.transaction_type == transaction.transaction_type),
            )
        )
        .order_by(asc(Rule.priority), asc(Rule.id))
        .all()
    )

    # Rules with no actions are effectively no-ops; skip them entirely to avoid wasting resources.
    rules = [r for r in rules if r and getattr(r, "actions", None)]

    if not rules:
        if not transaction.error_code:
            transaction.error_code = "NO_RULES"
        if not transaction.error_message:
            transaction.error_message = "No active rules matched this transaction."
        else:
            transaction.error_message = f"{transaction.error_message} No active rules matched this transaction."

        transaction.status = "PROCESSED"
        return

    had_rule_failures = False
    for rule in rules:

        try:
            matched = _evaluate_condition_block(db, customer, transaction, rule.conditions)
            if not matched:
                execution = TransactionRuleExecution(
                    transaction_id=transaction.id,
                    rule_id=rule.id,
                    result="SKIPPED",
                    details={"matched": False},
                )
                db.add(execution)
                continue

            executed_actions = []
            execution = TransactionRuleExecution(
                transaction_id=transaction.id,
                rule_id=rule.id,
                result="SUCCESS",
                details={"matched": True, "actions": []},
            )
            db.add(execution)
            db.flush()

            payload = transaction.payload if isinstance(transaction.payload, dict) else {}
            ctx = payload.get("_ruleContext") if isinstance(payload.get("_ruleContext"), dict) else {}
            ctx["rule_id"] = str(rule.id)
            ctx["rule_execution_id"] = str(execution.id)
            payload["_ruleContext"] = ctx
            transaction.payload = payload

            with db.begin_nested():
                executed_actions = _execute_actions(db, customer, transaction, rule.actions)
                db.flush()

            execution.details = {"matched": True, "actions": executed_actions}

        except Exception as e:
            had_rule_failures = True
            execution = TransactionRuleExecution(
                transaction_id=transaction.id,
                rule_id=rule.id,
                result="FAILED",
                details={"error": str(e)},
            )
            db.add(execution)

    transaction.status = "PROCESSED_WITH_ERRORS" if had_rule_failures else "PROCESSED"
