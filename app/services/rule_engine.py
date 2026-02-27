from datetime import datetime, timedelta

from sqlalchemy.orm import Session
from sqlalchemy import asc, extract

from app.models.bonus_award import BonusAward
from app.models.bonus_definition import BonusDefinition
from app.models.customer_tag import CustomerTag
from app.models.loyalty_tier import LoyaltyTier
from app.models.rule import Rule
from app.models.transaction_rule_execution import TransactionRuleExecution
from app.models.point_movement import PointMovement
from app.services.contact_service import get_customer
from app.services.loyalty_service import earn_points, burn_points
from app.services.reward_service import issue_reward, redeem_reward


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


def _as_int(value):
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def _compute_period_key(policy: str, now: datetime) -> str | None:
    if not policy:
        return None

    p = policy.strip().upper()
    d = now.date()

    if p == "ONCE_EVER":
        return None
    if p == "ONCE_PER_YEAR":
        return f"{d.year}"
    if p == "ONCE_PER_MONTH":
        return f"{d.year:04d}-{d.month:02d}"
    if p == "ONCE_PER_WEEK":
        iso = d.isocalendar()
        return f"{iso.year:04d}-W{iso.week:02d}"
    if p == "ONCE_PER_DAY":
        return d.isoformat()

    raise ValueError(f"Unsupported bonus award_policy: {policy}")


def _evaluate_condition_block(db: Session, customer, transaction, conditions) -> bool:
    if not conditions:
        return True

    if isinstance(conditions, list):
        return all(_evaluate_condition_block(db, customer, transaction, c) for c in conditions)

    if not isinstance(conditions, dict):
        return False

    if "all" in conditions:
        items = conditions.get("all") or []
        return all(_evaluate_condition_block(db, customer, transaction, c) for c in items)

    if "any" in conditions:
        items = conditions.get("any") or []
        return any(_evaluate_condition_block(db, customer, transaction, c) for c in items)

    if "not" in conditions:
        return not _evaluate_condition_block(db, customer, transaction, conditions.get("not"))

    payload = transaction.payload or {}

    payload_matches = conditions.get("payload")
    if payload_matches:
        if not isinstance(payload_matches, dict):
            return False

        for k, expected in payload_matches.items():
            if _get_by_path(payload, k) != expected:
                return False

    customer_status_in = conditions.get("customer_status_in")
    if customer_status_in is not None:
        if not isinstance(customer_status_in, list):
            return False
        if customer.status not in customer_status_in:
            return False

    created_days_gte = conditions.get("customer_created_days_gte")
    if created_days_gte is not None:
        try:
            days = int(created_days_gte)
        except Exception:
            return False
        if not customer.created_at:
            return False
        if customer.created_at > (datetime.utcnow() - timedelta(days=days)):
            return False

    last_activity_days_gte = conditions.get("customer_last_activity_days_gte")
    if last_activity_days_gte is not None:
        try:
            days = int(last_activity_days_gte)
        except Exception:
            return False
        if not customer.last_activity_at:
            return False
        if customer.last_activity_at > (datetime.utcnow() - timedelta(days=days)):
            return False

    customer_cmp = conditions.get("customer_cmp")
    if customer_cmp is not None:
        if not isinstance(customer_cmp, dict):
            return False
        field = customer_cmp.get("field")
        op = (customer_cmp.get("op") or "").lower()
        val = customer_cmp.get("value")
        if not field or not op:
            return False
        actual = getattr(customer, field, None)
        if op == "eq":
            if actual != val:
                return False
        else:
            a = _as_int(actual)
            if a is None:
                try:
                    a = float(actual)
                except Exception:
                    return False
            if op in {"gte", "lte"}:
                try:
                    b = float(val)
                except Exception:
                    return False
                if op == "gte" and not (a >= b):
                    return False
                if op == "lte" and not (a <= b):
                    return False
            elif op == "between":
                if not isinstance(val, list) or len(val) != 2:
                    return False
                try:
                    lo = float(val[0])
                    hi = float(val[1])
                except Exception:
                    return False
                if not (lo <= a <= hi):
                    return False
            else:
                return False

    payload_in = conditions.get("payload_in")
    if payload_in is not None:
        if not isinstance(payload_in, dict):
            return False
        for k, allowed in payload_in.items():
            if not isinstance(allowed, list) or not allowed:
                return False
            v = _get_by_path(payload, k)
            if v not in allowed:
                return False

    payload_contains = conditions.get("payload_contains")
    if payload_contains is not None:
        if not isinstance(payload_contains, dict):
            return False
        for k, needle in payload_contains.items():
            hay = _get_by_path(payload, k)
            if hay is None:
                return False
            if isinstance(hay, list):
                if needle not in hay:
                    return False
            elif isinstance(hay, str):
                if str(needle) not in hay:
                    return False
            else:
                return False

    payload_cmp = conditions.get("payload_cmp")
    if payload_cmp is not None:
        if not isinstance(payload_cmp, dict):
            return False
        path = payload_cmp.get("path")
        op = (payload_cmp.get("op") or "").lower()
        val = payload_cmp.get("value")
        if not path or not op:
            return False
        actual = _get_by_path(payload, path)
        if op == "eq":
            if actual != val:
                return False
        else:
            a = _as_int(actual)
            if a is None:
                try:
                    a = float(actual)
                except Exception:
                    return False
            if op in {"gte", "lte"}:
                try:
                    b = float(val)
                except Exception:
                    return False
                if op == "gte" and not (a >= b):
                    return False
                if op == "lte" and not (a <= b):
                    return False
            elif op == "between":
                if not isinstance(val, list) or len(val) != 2:
                    return False
                try:
                    lo = float(val[0])
                    hi = float(val[1])
                except Exception:
                    return False
                if not (lo <= a <= hi):
                    return False
            else:
                return False

    payload_present = conditions.get("payload_present")
    if payload_present:
        if not isinstance(payload_present, list):
            return False
        for k in payload_present:
            if _get_by_path(payload, k) in (None, ""):
                return False

    amount_gte = conditions.get("amount_gte")
    if amount_gte is not None:
        amount = _as_int(_get_by_path(payload, "amount"))
        if amount is None or amount < int(amount_gte):
            return False

    points_gte = conditions.get("points_gte")
    if points_gte is not None:
        points = _as_int(_get_by_path(payload, "points"))
        if points is None or points < int(points_gte):
            return False

    loyalty_status_in = conditions.get("customer_loyalty_status_in")
    if loyalty_status_in is not None:
        if not isinstance(loyalty_status_in, list):
            return False
        if customer.loyalty_status not in loyalty_status_in:
            return False

    lifetime_points_gte = conditions.get("customer_lifetime_points_gte")
    if lifetime_points_gte is not None:
        lp = customer.lifetime_points or 0
        if lp < int(lifetime_points_gte):
            return False

    weekday_in = conditions.get("weekday_in")
    if weekday_in is not None:
        if not isinstance(weekday_in, list):
            return False
        if datetime.utcnow().weekday() not in set(int(x) for x in weekday_in):
            return False

    if conditions.get("first_purchase"):
        existing_earn = (
            db.query(PointMovement.id)
            .filter(PointMovement.customer_id == customer.id)
            .filter(PointMovement.type == "EARN")
            .first()
        )
        if existing_earn:
            return False

    if conditions.get("birthday"):
        if not customer.birthdate:
            return False
        today = datetime.utcnow().date()
        birth = customer.birthdate
        if not (today.day == birth.day and today.month == birth.month):
            return False

        # Si un bonus spécifique a été défini via la règle, on vérifie cette valeur.
        # Sinon, fallback (moins strict) : un EARN quelconque a déjà été donné cette année.
        birthday_bonus_points = conditions.get("birthday_bonus_points")
        q = (
            db.query(PointMovement.id)
            .filter(PointMovement.customer_id == customer.id)
            .filter(PointMovement.type == "EARN")
            .filter(extract("year", PointMovement.created_at) == today.year)
        )
        if birthday_bonus_points is not None:
            q = q.filter(PointMovement.points == int(birthday_bonus_points))
        already_given = q.first()
        if already_given:
            return False

    # ------------------------------------------------------------
    # ✅ Bonus campagne déjà attribué (paramétrable)
    # ------------------------------------------------------------
    # Usage:
    #   {"earn_points_awarded_this_year": {"points": 50}}
    # ou:
    #   {"earn_points_awarded_this_year": 50}
    awarded = conditions.get("earn_points_awarded_this_year")
    if awarded is not None:
        points_value = awarded
        if isinstance(awarded, dict):
            points_value = awarded.get("points")

        points_value = _as_int(points_value)
        if points_value is None:
            return False

        today = datetime.utcnow().date()
        already_awarded = (
            db.query(PointMovement.id)
            .filter(PointMovement.customer_id == customer.id)
            .filter(PointMovement.type == "EARN")
            .filter(extract("year", PointMovement.created_at) == today.year)
            .filter(PointMovement.points == points_value)
            .first()
        )
        if not already_awarded:
            return False

    # ------------------------------------------------------------
    # ✅ Bonus ledger (bonus_definitions + bonus_awards)
    # ------------------------------------------------------------
    # Usage:
    #   {"bonus_awarded": {"bonusKey": "BIRTHDAY_200"}}
    bonus_awarded = conditions.get("bonus_awarded")
    if bonus_awarded is not None:
        if isinstance(bonus_awarded, str):
            bonus_key = bonus_awarded
        elif isinstance(bonus_awarded, dict):
            bonus_key = bonus_awarded.get("bonusKey") or bonus_awarded.get("bonus_key")
        else:
            return False

        if not bonus_key:
            return False

        definition = (
            db.query(BonusDefinition)
            .filter(BonusDefinition.bonus_key == bonus_key)
            .filter(BonusDefinition.active.is_(True))
            .filter(BonusDefinition.brand == transaction.brand)
            .first()
        )
        if not definition:
            definition = (
                db.query(BonusDefinition)
                .filter(BonusDefinition.bonus_key == bonus_key)
                .filter(BonusDefinition.active.is_(True))
                .filter(BonusDefinition.brand.is_(None))
                .first()
            )
        if not definition:
            return False

        # brand scoping: if definition.brand is set, it must match transaction/customer brand
        if definition.brand and definition.brand != transaction.brand:
            return False

        now = datetime.utcnow()
        period_key = _compute_period_key(definition.award_policy, now)

        q = (
            db.query(BonusAward.id)
            .filter(BonusAward.bonus_key == bonus_key)
            .filter(BonusAward.brand == transaction.brand)
            .filter(BonusAward.profile_id == transaction.profile_id)
        )
        if period_key is None:
            q = q.filter(BonusAward.period_key.is_(None))
        else:
            q = q.filter(BonusAward.period_key == period_key)

        already = q.first()
        if not already:
            return False

    return True


def _execute_actions(db: Session, customer, transaction, actions):
    if actions is None:
        return []
    if isinstance(actions, dict):
        actions = [actions]
    if not isinstance(actions, list):
        raise ValueError("Invalid actions format")

    executed = []
    for action in actions:
        if not isinstance(action, dict):
            raise ValueError("Invalid action")

        action_type = action.get("type")
        if action_type == "earn_points":
            points = action.get("points")

            depth = _as_int(_get_by_path(transaction.payload or {}, "_ruleDepth")) or 0

            fake_tx = type(
                "FakeTx",
                (),
                {"id": transaction.id, "payload": {"amount": points, "_ruleDepth": depth}},
            )
            earn_points(db, customer, fake_tx)
            executed.append({"type": action_type, "points": _as_int(points)})

        elif action_type == "earn_points_from_amount":
            amount_path = action.get("amount_path")
            if not amount_path:
                raise ValueError("earn_points_from_amount requires amount_path")
            rate = action.get("rate")
            if rate is None:
                raise ValueError("earn_points_from_amount requires rate")
            try:
                rate_f = float(rate)
            except Exception:
                raise ValueError("earn_points_from_amount rate must be a number")

            raw_amount = _get_by_path(transaction.payload or {}, amount_path)
            try:
                amount_f = float(raw_amount)
            except Exception:
                raise ValueError("earn_points_from_amount amount is missing or not numeric")

            points = int(amount_f * rate_f)
            if action.get("min_points") is not None:
                points = max(points, int(action.get("min_points")))
            if action.get("max_points") is not None:
                points = min(points, int(action.get("max_points")))

            depth = _as_int(_get_by_path(transaction.payload or {}, "_ruleDepth")) or 0
            fake_tx = type(
                "FakeTx",
                (),
                {"id": transaction.id, "payload": {"amount": points, "_ruleDepth": depth}},
            )
            earn_points(db, customer, fake_tx)
            executed.append(
                {
                    "type": action_type,
                    "amountPath": amount_path,
                    "rate": rate_f,
                    "points": int(points),
                    "minPoints": action.get("min_points"),
                    "maxPoints": action.get("max_points"),
                }
            )

        elif action_type == "burn_points":
            points = action.get("points")

            depth = _as_int(_get_by_path(transaction.payload or {}, "_ruleDepth")) or 0

            fake_tx = type(
                "FakeTx",
                (),
                {"id": transaction.id, "payload": {"points": points, "_ruleDepth": depth}},
            )
            burn_points(db, customer, fake_tx)
            executed.append({"type": action_type, "points": _as_int(points)})

        elif action_type == "redeem_reward":
            reward_id = action.get("reward_id") or action.get("rewardId") or action.get("rewardID")
            if isinstance(reward_id, dict):
                reward_id = reward_id.get("id") or reward_id.get("rewardId") or reward_id.get("reward_id")
            if reward_id is not None:
                reward_id = str(reward_id)
            redeem_reward(db, customer, transaction, reward_id=reward_id)
            executed.append({"type": action_type})

        elif action_type == "issue_reward":
            reward_id = action.get("reward_id") or action.get("rewardId") or action.get("rewardID")
            if isinstance(reward_id, dict):
                reward_id = reward_id.get("id") or reward_id.get("rewardId") or reward_id.get("reward_id")
            if reward_id is not None:
                reward_id = str(reward_id)
            issue_reward(db, customer, transaction, reward_id=reward_id)
            executed.append({"type": action_type, "rewardId": reward_id})

        elif action_type == "record_bonus_award":
            bonus_key = action.get("bonusKey") or action.get("bonus_key")
            if not bonus_key:
                raise ValueError("record_bonus_award requires bonusKey")

            definition = (
                db.query(BonusDefinition)
                .filter(BonusDefinition.bonus_key == bonus_key)
                .filter(BonusDefinition.active.is_(True))
                .filter(BonusDefinition.brand == transaction.brand)
                .first()
            )
            if not definition:
                definition = (
                    db.query(BonusDefinition)
                    .filter(BonusDefinition.bonus_key == bonus_key)
                    .filter(BonusDefinition.active.is_(True))
                    .filter(BonusDefinition.brand.is_(None))
                    .first()
                )
            if not definition:
                raise ValueError(f"Unknown bonusKey: {bonus_key}")

            if definition.brand and definition.brand != transaction.brand:
                raise ValueError("bonusKey brand mismatch")

            now = datetime.utcnow()
            period_key = _compute_period_key(definition.award_policy, now)

            q = (
                db.query(BonusAward.id)
                .filter(BonusAward.bonus_key == bonus_key)
                .filter(BonusAward.brand == transaction.brand)
                .filter(BonusAward.profile_id == transaction.profile_id)
            )
            if period_key is None:
                q = q.filter(BonusAward.period_key.is_(None))
            else:
                q = q.filter(BonusAward.period_key == period_key)

            existing = q.first()
            if not existing:
                award = BonusAward(
                    bonus_key=bonus_key,
                    brand=transaction.brand,
                    profile_id=transaction.profile_id,
                    period_key=period_key,
                    event_id=transaction.event_id,
                    transaction_id=transaction.id,
                    meta={"eventType": transaction.event_type},
                )
                db.add(award)

            executed.append(
                {
                    "type": action_type,
                    "bonusKey": bonus_key,
                    "awardPolicy": definition.award_policy,
                    "periodKey": period_key,
                    "idempotent": bool(existing),
                }
            )

        elif action_type == "reset_status_points":
            customer.status_points = 0
            db.flush()

            from app.services.loyalty_status_service import update_customer_status

            depth = _as_int(_get_by_path(transaction.payload or {}, "_ruleDepth")) or 0
            update_customer_status(
                db,
                customer,
                reason="RESET",
                source_transaction_id=transaction.id,
                depth=depth,
            )
            executed.append({"type": action_type})

        elif action_type == "downgrade_one_tier":
            # Downgrade by one tier rank for the customer's brand.
            current = (
                db.query(LoyaltyTier)
                .filter(LoyaltyTier.brand == customer.brand)
                .filter(LoyaltyTier.active.is_(True))
                .filter(LoyaltyTier.key == customer.loyalty_status)
                .first()
            )
            if not current:
                raise ValueError("Current loyalty tier not found for brand")

            target = (
                db.query(LoyaltyTier)
                .filter(LoyaltyTier.brand == customer.brand)
                .filter(LoyaltyTier.active.is_(True))
                .filter(LoyaltyTier.rank == int(current.rank) - 1)
                .first()
            )
            if not target:
                executed.append({"type": action_type, "changed": False, "reason": "Already at lowest tier"})
                continue

            customer.status_points = int(target.min_status_points)
            db.flush()

            from app.services.loyalty_status_service import update_customer_status

            depth = _as_int(_get_by_path(transaction.payload or {}, "_ruleDepth")) or 0
            update_customer_status(
                db,
                customer,
                reason="INACTIVITY",
                source_transaction_id=transaction.id,
                depth=depth,
            )
            executed.append({"type": action_type, "changed": True, "toTier": target.key})

        elif action_type == "set_customer_status":
            status = action.get("status")
            if not status:
                raise ValueError("set_customer_status requires status")
            customer.status = str(status)
            db.flush()
            executed.append({"type": action_type, "status": customer.status})

        elif action_type == "add_customer_tag":
            tag = action.get("tag")
            if not tag:
                raise ValueError("add_customer_tag requires tag")
            tag = str(tag)
            existing = (
                db.query(CustomerTag.id)
                .filter(CustomerTag.customer_id == customer.id)
                .filter(CustomerTag.tag == tag)
                .first()
            )
            if not existing:
                db.add(CustomerTag(customer_id=customer.id, tag=tag))
            executed.append({"type": action_type, "tag": tag, "idempotent": bool(existing)})

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

    rules = (
        db.query(Rule)
        .filter(
            Rule.brand == transaction.brand,
            Rule.event_type == transaction.event_type,
            Rule.active == True,
        )
        .order_by(asc(Rule.priority), asc(Rule.id))
        .all()
    )

    if not rules:
        if not transaction.error_code:
            transaction.error_code = "NO_RULES"
        if not transaction.error_message:
            transaction.error_message = "No active rules matched this event."
        else:
            transaction.error_message = f"{transaction.error_message} No active rules matched this event."

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

            if not rule.actions:
                execution = TransactionRuleExecution(
                    transaction_id=transaction.id,
                    rule_id=rule.id,
                    result="SKIPPED",
                    details={"matched": True, "reason": "No actions defined"},
                )
                db.add(execution)
                continue

            executed_actions = []
            with db.begin_nested():
                executed_actions = _execute_actions(db, customer, transaction, rule.actions)
                db.flush()

            execution = TransactionRuleExecution(
                transaction_id=transaction.id,
                rule_id=rule.id,
                result="SUCCESS",
                details={"matched": True, "actions": executed_actions},
            )
            db.add(execution)
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
