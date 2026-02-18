from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import extract, and_

from app.models.campaign import Campaign
from app.models.point_movement import PointMovement
from app.services.loyalty_service import earn_points


def apply_campaigns(db: Session, customer, transaction):
    now = datetime.utcnow()

    campaigns = (
        db.query(Campaign)
        .filter(
            Campaign.brand == transaction.brand,
            Campaign.event_type == transaction.event_type,
            Campaign.active.is_(True),
            (Campaign.start_date.is_(None)) | (Campaign.start_date <= now),
            (Campaign.end_date.is_(None)) | (Campaign.end_date >= now),
        )
        .all()
    )

    applied = []

    for campaign in campaigns:
        conditions = campaign.conditions or {}
        payload = transaction.payload or {}

        # ============================================================
        # 🟣 WEEKEND
        # ============================================================
        if conditions.get("weekend"):
            # 5 = samedi, 6 = dimanche
            if now.weekday() not in (5, 6):
                continue

        # ============================================================
        # 🟣 FIRST PURCHASE (vérification en base, pas dans le payload)
        # ============================================================
        if conditions.get("first_purchase"):
            existing_earn = (
                db.query(PointMovement.id)
                .filter(PointMovement.customer_id == customer.id)
                .filter(PointMovement.type == "EARN")
                .first()
            )

            # s’il existe déjà un earn → pas premier achat
            if existing_earn:
                continue

        # ============================================================
        # 🟣 BIRTHDAY
        # ============================================================
        if conditions.get("birthday"):
            if not customer.birthdate:
                continue

            today = now.date()
            birth = customer.birthdate

            # même jour + mois
            if not (today.day == birth.day and today.month == birth.month):
                continue

            # vérifier si bonus anniversaire déjà donné cette année
            already_given = (
                db.query(PointMovement.id)
                .filter(PointMovement.customer_id == customer.id)
                .filter(PointMovement.type == "EARN")
                .filter(extract("year", PointMovement.created_at) == today.year)
                .filter(PointMovement.points == campaign.bonus_points)
                .first()
            )

            if already_given:
                continue

        # ============================================================
        # 🟣 APPLICATION DU BONUS
        # ============================================================
        bonus_payload = {"amount": campaign.bonus_points}

        # transaction minimale isolée
        class FakeTx:
            id = transaction.id
            payload = bonus_payload

        movement = earn_points(db, customer, FakeTx)
        applied.append((campaign, movement))

    return applied
