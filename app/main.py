import base64
import hmac
import os

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from starlette.requests import Request
from app.db import engine, Base

from app.models.transaction import Transaction
from app.models.rule import Rule
from app.models.transaction_rule_execution import TransactionRuleExecution
from app.models.customer import Customer
from app.models.point_movement import PointMovement
from app.models.cash_movement import CashMovement
from app.models.reward import Reward
from app.models.customer_reward import CustomerReward
from app.models.internal_job import InternalJob
from app.models.event_type import TransactionType
from app.models.loyalty_tier import LoyaltyTier

from app.routes.wallet import router as wallet_router
from app.routes.customers import router as customers_router
from app.routes.transactions import router as transactions_router
from app.routes.rules import router as rules_router
from app.routes.rewards import router as rewards_router
from app.routes.imports import router as imports_router
from app.routes.admin import router as admin_router
from app.routes.internal_jobs import router as internal_jobs_router
from app.routes.event_types import router as transaction_types_router
from app.routes.loyalty_tiers import router as loyalty_tiers_router

app = FastAPI(title="Loyalty Engine")


@app.middleware("http")
async def basic_auth_middleware(request: Request, call_next):
    public_paths = {"/", "/docs", "/openapi.json", "/redoc", "/favicon.ico"}
    if request.url.path in public_paths:
        return await call_next(request)

    username = os.getenv("API_BASIC_AUTH_USERNAME", "karaf")
    password = os.getenv("API_BASIC_AUTH_PASSWORD", "karaf")

    auth = request.headers.get("authorization") or ""
    if not auth.lower().startswith("basic "):
        return JSONResponse(
            status_code=401,
            content={"detail": "Not authenticated"},
            headers={"WWW-Authenticate": 'Basic realm="Loyalty Engine"'},
        )

    try:
        token = auth.split(" ", 1)[1].strip()
        raw = base64.b64decode(token).decode("utf-8")
        provided_user, provided_pass = raw.split(":", 1)
    except Exception:
        return JSONResponse(
            status_code=401,
            content={"detail": "Invalid authentication credentials"},
            headers={"WWW-Authenticate": 'Basic realm="Loyalty Engine"'},
        )

    if not (
        hmac.compare_digest(provided_user, username)
        and hmac.compare_digest(provided_pass, password)
    ):
        return JSONResponse(
            status_code=401,
            content={"detail": "Invalid authentication credentials"},
            headers={"WWW-Authenticate": 'Basic realm="Loyalty Engine"'},
        )

    return await call_next(request)


@app.on_event("startup")
def startup():
    Base.metadata.create_all(bind=engine)


app.include_router(wallet_router)
app.include_router(customers_router)
app.include_router(transactions_router)
app.include_router(rules_router)
app.include_router(rewards_router)
app.include_router(imports_router)
app.include_router(admin_router)
app.include_router(internal_jobs_router)
app.include_router(transaction_types_router)
app.include_router(loyalty_tiers_router)


@app.get("/")
def read_root():
    return {"message": "Loyalty Engine is running"}
