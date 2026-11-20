import base64
import hmac
import os
import re
import time
import uuid

import logging

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy.exc import IntegrityError, ProgrammingError, SQLAlchemyError
from app.db import engine, Base

from app.models.transaction import Transaction
from app.models.rule import Rule
from app.models.transaction_rule_execution import TransactionRuleExecution
from app.models.customer import Customer
from app.models.point_movement import PointMovement
from app.models.purchase_movement import PurchaseMovement
from app.models.cash_movement import CashMovement
from app.models.reward import Reward
from app.models.customer_reward import CustomerReward
from app.models.coupon_type import CouponType
from app.models.coupon_type_reward import CouponTypeReward
from app.models.customer_coupon import CustomerCoupon
from app.models.internal_job import InternalJob
from app.models.event_type import TransactionType
from app.models.loyalty_tier import LoyaltyTier
from app.models.brand_loyalty_settings import BrandLoyaltySettings
from app.models.customer_loyalty_guarantee import CustomerLoyaltyGuarantee
from app.models.api_client import ApiClient  # noqa: F401
from app.models.redemption import Redemption  # noqa: F401
from app.models.webhook_endpoint import WebhookEndpoint  # noqa: F401
from app.models.webhook_outbox import WebhookOutbox  # noqa: F401
from app.models.storefront_audit_event import StorefrontAuditEvent  # noqa: F401

from app.routes.customers import router as customers_router
from app.routes.transactions import router as transactions_router
from app.routes.rules import router as rules_router
from app.routes.rewards import router as rewards_router
from app.routes.coupon_types import router as coupon_types_router
from app.routes.product_categories import router as product_categories_router
from app.routes.products import router as products_router
from app.routes.segments import router as segments_router
from app.routes.imports import router as imports_router
from app.routes.admin import router as admin_router
from app.routes.internal_jobs import router as internal_jobs_router
from app.routes.event_types import router as transaction_types_router
from app.routes.loyalty_tiers import router as loyalty_tiers_router
from app.routes.unomi_integrations import router as unomi_integrations_router
from app.routes.storefront import router as storefront_router

def _environment() -> str:
    return (os.getenv("ENVIRONMENT") or os.getenv("ENV") or "development").strip().lower()


def _is_production() -> bool:
    return _environment() in {"production", "prod"}


_docs_enabled = not _is_production()
app = FastAPI(
    title="Loyalty Engine",
    docs_url="/docs" if _docs_enabled else None,
    redoc_url="/redoc" if _docs_enabled else None,
    openapi_url="/openapi.json" if _docs_enabled else None,
)

# ─── CORS ─────────────────────────────────────────────────────────
_DEFAULT_CORS_ORIGINS = [
    "http://localhost:3000",
    "https://localhost:3000",
    "http://127.0.0.1:3000",
    "https://127.0.0.1:3000",
    "https://amplify.qilinsa.com",
    "https://uat.amplify.qilinsa.com",
]


def _build_cors_origins() -> list[str]:
    extra = os.getenv("CORS_ALLOW_ORIGINS", "")
    merged = list(_DEFAULT_CORS_ORIGINS)
    for origin in extra.split(","):
        o = origin.strip()
        if o and o not in merged:
            merged.append(o)
    return merged


CORS_ALLOWED_ORIGINS = _build_cors_origins()
# Optional: e.g. https://([a-z0-9-]+\.)*amplify\.qilinsa\.com — set "" to disable regex.
CORS_ORIGIN_REGEX = (os.getenv("CORS_ALLOW_ORIGIN_REGEX") or "").strip() or None

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ALLOWED_ORIGINS,
    allow_origin_regex=CORS_ORIGIN_REGEX,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Unomi-Sync", "X-Unomi-Sync-Detail", "X-Total-Count"],
)


def _cors_headers_for_request(request: Request) -> dict[str, str]:
    """Ensure browser sees CORS on 401/early responses (not only 200 from routes)."""
    origin = request.headers.get("origin")
    if not origin:
        return {}
    if origin in CORS_ALLOWED_ORIGINS:
        return {
            "Access-Control-Allow-Origin": origin,
            "Access-Control-Allow-Credentials": "true",
            "Vary": "Origin",
        }
    if CORS_ORIGIN_REGEX and re.fullmatch(CORS_ORIGIN_REGEX, origin):
        return {
            "Access-Control-Allow-Origin": origin,
            "Access-Control-Allow-Credentials": "true",
            "Vary": "Origin",
        }
    return {}


def _error_id() -> str:
    return str(uuid.uuid4())


def _db_error_response(request: Request, *, status_code: int, detail: str, error_id: str) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={"detail": detail, "error_id": error_id},
        headers=_cors_headers_for_request(request),
    )


@app.exception_handler(ProgrammingError)
async def db_programming_error_handler(request: Request, exc: ProgrammingError):
    """Missing tables/columns on prod (migrations not applied) surfaces as 500 otherwise."""
    error_id = _error_id()
    logging.exception("Database schema error error_id=%s on %s %s", error_id, request.method, request.url.path)
    return _db_error_response(
        request,
        status_code=503,
        detail=(
            "Database schema is out of date for this API version. "
            "On the server run: alembic upgrade head"
        ),
        error_id=error_id,
    )


@app.exception_handler(IntegrityError)
async def db_integrity_error_handler(request: Request, exc: IntegrityError):
    error_id = _error_id()
    logging.exception("Integrity error error_id=%s on %s %s", error_id, request.method, request.url.path)
    return _db_error_response(
        request,
        status_code=409,
        detail="Database integrity constraint violated",
        error_id=error_id,
    )


@app.exception_handler(SQLAlchemyError)
async def db_sqlalchemy_error_handler(request: Request, exc: SQLAlchemyError):
    if isinstance(exc, (ProgrammingError, IntegrityError)):
        raise exc
    error_id = _error_id()
    logging.exception("Database error error_id=%s on %s %s", error_id, request.method, request.url.path)
    return _db_error_response(
        request,
        status_code=503,
        detail="Database error",
        error_id=error_id,
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    error_id = _error_id()
    logging.exception("Unhandled error error_id=%s on %s %s", error_id, request.method, request.url.path)
    return JSONResponse(
        status_code=500,
        content={
            "detail": "Internal server error",
            "error_id": error_id,
        },
        headers=_cors_headers_for_request(request),
    )


@app.middleware("http")
async def timing_middleware(request: Request, call_next):
    started = time.perf_counter()
    response = await call_next(request)
    elapsed_ms = (time.perf_counter() - started) * 1000
    if elapsed_ms >= 500:
        logging.warning(
            "slow request %s %s %.0fms",
            request.method,
            request.url.path,
            elapsed_ms,
        )
    response.headers["X-Process-Time-Ms"] = f"{elapsed_ms:.0f}"
    return response


@app.middleware("http")
async def basic_auth_middleware(request: Request, call_next):
    public_paths = {"/", "/favicon.ico"}
    if _docs_enabled:
        public_paths.update({"/docs", "/openapi.json", "/redoc"})
    if request.url.path in public_paths or request.url.path.startswith("/v1/storefront"):
        return await call_next(request)

    # Always allow CORS preflight requests — browsers never send credentials on OPTIONS
    if request.method == "OPTIONS":
        return await call_next(request)

    username = os.getenv("API_BASIC_AUTH_USERNAME", "karaf")
    password = os.getenv("API_BASIC_AUTH_PASSWORD")
    if not password:
        if _is_production():
            return JSONResponse(
                status_code=503,
                content={"detail": "Authentication is not configured"},
                headers=_cors_headers_for_request(request),
            )
        password = "karaf"

    auth = request.headers.get("authorization") or ""
    if not auth.lower().startswith("basic "):
        return JSONResponse(
            status_code=401,
            content={"detail": "Not authenticated"},
            headers={
                "WWW-Authenticate": 'Basic realm="Loyalty Engine"',
                **_cors_headers_for_request(request),
            },
        )

    try:
        token = auth.split(" ", 1)[1].strip()
        raw = base64.b64decode(token).decode("utf-8")
        provided_user, provided_pass = raw.split(":", 1)
    except Exception:
        return JSONResponse(
            status_code=401,
            content={"detail": "Invalid authentication credentials"},
            headers={
                "WWW-Authenticate": 'Basic realm="Loyalty Engine"',
                **_cors_headers_for_request(request),
            },
        )

    if not (
        hmac.compare_digest(provided_user, username)
        and hmac.compare_digest(provided_pass, password)
    ):
        return JSONResponse(
            status_code=401,
            content={"detail": "Invalid authentication credentials"},
            headers={
                "WWW-Authenticate": 'Basic realm="Loyalty Engine"',
                **_cors_headers_for_request(request),
            },
        )

    return await call_next(request)


@app.on_event("startup")
def startup():
    if _is_production():
        password = (os.getenv("API_BASIC_AUTH_PASSWORD") or "").strip()
        if not password or password == "karaf":
            raise RuntimeError("API_BASIC_AUTH_PASSWORD must be set in production")
        if not (os.getenv("UNOMI_WEBHOOK_SECRET") or "").strip():
            raise RuntimeError("UNOMI_WEBHOOK_SECRET must be set in production")
        return
    auto_create = (os.getenv("AUTO_CREATE_SCHEMA", "true") or "true").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if auto_create:
        Base.metadata.create_all(bind=engine)


app.include_router(customers_router)
app.include_router(transactions_router)
app.include_router(rules_router)
app.include_router(rewards_router)
app.include_router(coupon_types_router)
app.include_router(product_categories_router)
app.include_router(products_router)
app.include_router(segments_router)
app.include_router(imports_router)
app.include_router(admin_router)
app.include_router(internal_jobs_router)
app.include_router(transaction_types_router)
app.include_router(loyalty_tiers_router)
app.include_router(unomi_integrations_router)
app.include_router(storefront_router)


@app.get("/")
def read_root():
    return {"message": "Loyalty Engine is running"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8001, reload=True)
