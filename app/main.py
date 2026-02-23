from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.db import engine, Base

from app.models.transaction import Transaction
from app.models.rule import Rule
from app.models.transaction_rule_execution import TransactionRuleExecution
from app.models.customer import Customer
from app.models.point_movement import PointMovement
from app.models.reward import Reward
from app.models.customer_reward import CustomerReward
from app.models.internal_job import InternalJob
from app.models.event_type import EventType
from app.models.bonus_definition import BonusDefinition
from app.models.bonus_award import BonusAward
from app.models.loyalty_tier import LoyaltyTier
from app.models.customer_tag import CustomerTag

from app.routes.events import router as events_router
from app.routes.wallet import router as wallet_router
from app.routes.customers import router as customers_router
from app.routes.transactions import router as transactions_router
from app.routes.rules import router as rules_router
from app.routes.rewards import router as rewards_router
from app.routes.imports import router as imports_router
from app.routes.admin import router as admin_router
from app.routes.internal_jobs import router as internal_jobs_router
from app.routes.event_types import router as event_types_router
from app.routes.bonus_definitions import router as bonus_definitions_router
from app.routes.bonus_awards import router as bonus_awards_router
from app.routes.loyalty_tiers import router as loyalty_tiers_router

app = FastAPI(title="Loyalty Engine")

# ─── CORS ─────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "https://localhost:3000",
        "http://127.0.0.1:3000",
        "https://127.0.0.1:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def startup():
    Base.metadata.create_all(bind=engine)


app.include_router(events_router)
app.include_router(wallet_router)
app.include_router(customers_router)
app.include_router(transactions_router)
app.include_router(rules_router)
app.include_router(rewards_router)
app.include_router(imports_router)
app.include_router(admin_router)
app.include_router(internal_jobs_router)
app.include_router(event_types_router)
app.include_router(bonus_definitions_router)
app.include_router(bonus_awards_router)
app.include_router(loyalty_tiers_router)


@app.get("/")
def read_root():
    return {"message": "Loyalty Engine is running"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8001, reload=True)
