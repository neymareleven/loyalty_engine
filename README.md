 # Loyalty Engine
 
 Backend API for a multi-brand loyalty engine.
 
 ## Key features
 
 - **Brand scoping** for admin operations via `X-Brand`.
 - **Multi-brand ingestion** for transactions/events (ingestion payload carries `event.brand`).
 - **Form-friendly admin APIs** for building Rules and Internal Jobs without manual JSON typing.
 
 ## Prerequisites
 
 - **Python** 3.11+ (3.12 recommended)
 - **PostgreSQL**
 - `DATABASE_URL` configured
 
 Dependencies are pinned in `requirements.txt`:
 
 - FastAPI + Uvicorn
 - SQLAlchemy + psycopg2
 - Alembic
 - Pydantic
 - python-dotenv
 
 ## Installation
 
 ```powershell
 python -m venv .venv
 .\.venv\Scripts\Activate.ps1
 pip install -r requirements.txt
 ```
 
 ## Configuration
 
 The app reads environment variables from `.env` (via `python-dotenv`).
 
 Required:
 
 - `DATABASE_URL`
   - Example:
     - `postgresql://postgres:postgres@localhost:5432/loyalty_engine`
 
 ## Database & migrations
 
 Alembic is configured (see `alembic.ini`, `alembic/env.py`). Apply migrations with:
 
 ```powershell
 alembic upgrade head
 ```
 
 Note: the app also calls `Base.metadata.create_all()` on startup (see `app/main.py`). In production, prefer Alembic-managed schema.
 
 ## Running the API locally
 
 ```bash
 uvicorn app.main:app --reload
 ```
 
 ## Core concepts (how the engine works)
 
 ### Entities
 
 - **Customer**: identified by `(brand, profile_id)`.
 - **Transaction**: an event ingested via `/transactions` (EXTERNAL) or emitted internally (INTERNAL jobs, admin actions).
 - **Rule**: evaluated on each ingested transaction of matching `transaction_type(s)`.
 - **Wallet / Point movements**: points ledger entries created by rule actions or admin overrides.
 - **Loyalty tiers**: compute `Customer.loyalty_status` from current points.
 - **Product catalog**: reference table used to compute points from purchased products.
 - **Segments**: audience targeting for rules and internal jobs.
 
 ### Data flow (typical)
 
 1. Ingest a transaction (`POST /transactions`).
 2. The engine loads active rules for the brand and the transaction type.
 3. Each rule is checked:
   - optional segment membership (`segment_ids`)
   - conditions (AST)
 4. Matching rules apply actions (earn points, issue coupon, reset status points).
 5. Wallet is updated and loyalty tier recomputed.
 
 ## Running the Internal Job scheduler (cron worker)
 
 Internal Jobs are executed automatically by a separate scheduler loop. In production, you typically run **two processes** on the same server:
 
 - API: `uvicorn app.main:app --host 0.0.0.0 --port 8000`
 - Scheduler worker: `python -m app.services.internal_job_scheduler`
 
 If the worker is not running, jobs will **not** execute automatically (you can still use `POST /admin/internal-jobs/{job_id}/run`).
 
 ## Selector / Condition AST format (Rules & Internal Jobs)
 
 Both Rules (`conditions`) and Internal Jobs (`selector`) use an AST structure to express boolean logic.
 
 - Combinators:
   - `{ "and": [<node>, <node>, ...] }`
   - `{ "or": [<node>, <node>, ...] }`
   - `{ "not": <node> }`
 - Leaf:
   - `{ "field": "customer.status", "operator": "in", "value": ["ACTIVE"] }`
 
 Notes:
 
 - Rule conditions can read `payload.*`, `customer.*`, `customer.metrics.*`.
 - Internal job selectors can read `customer.*`, `customer.metrics.*`, and `system.*`.
 
 ### Internal Job schedule format (cron)
 
 `InternalJob.schedule` is a structured object:
 
 ```json
 {
   "type": "cron",
   "cron": "*/2 * * * *",
   "timezone": "UTC"
 }
 ```
 
 - `type`: currently only `"cron"` is supported.
 - `cron`: standard 5-field cron expression.
 - `timezone`: optional. If omitted, the backend defaults to **UTC**.
 
 Health check:
 
 - `GET /` returns `{ "message": "Loyalty Engine is running" }`
 
 ## Brand scoping
 
 - **Admin routes** are scoped by the active brand.
 - The active brand is provided via the `X-Brand` header.
 - Some endpoints accept a `brand` field in payloads, but the backend validates it against `X-Brand` and rejects mismatches.
 
 ### Example header
 
 ```powershell
 -Headers @{ "X-Brand" = "BRAND_A" }
 ```
 
 ## Form-friendly UI catalogs (admin)
 
 The backend exposes JSON schema + UI hints endpoints that the frontend can consume to render forms.
 
 ### Rule Builder
 
 - `GET /admin/rules/ui-catalog`
   - Returns JSON schema + UI hints for `RuleCreate` and `RuleUpdate`.
   - `brand` is hidden in UI hints (brand comes from `X-Brand`).
   - `event_type` uses a remote select and displays `EventType.name`.
 
 - `GET /admin/rules/ui-bundle`
   - Single-call bundle for the Rule Builder.
 
 ### Internal Jobs
 
 - `GET /admin/internal-jobs/ui-catalog`
   - Includes:
     - `job`: JSON schema + UI hints for `InternalJobCreate`
     - `jobTypes`: UI presets to prefill job forms
     - `selector`: selector builder catalog
     - `payloadTemplate`: UI hints for payload template editing
 
 - `GET /admin/internal-jobs/ui-bundle`
   - Single-call bundle for the Internal Job Builder.
 
 - `POST /admin/internal-jobs/{job_id}/preview`
 - `POST /admin/internal-jobs/{job_id}/run`
 
 ## Loyalty tiers (loyalty_status)
 
 Customers have a `loyalty_status` (stored as a tier `key`) derived from their current `status_points`.
 
 ### Tier configuration rules (brand-scoped)
 
 - A **base tier is required**:
   - `rank = 0`
   - `min_status_points = 0`
 - `rank` and `min_status_points` must be **non-negative integers**.
 - Tiers must be **strictly increasing**:
   - When `rank` increases, `min_status_points` must strictly increase.
 - Uniqueness (per brand):
   - `(brand, rank)` is unique
   - `(brand, min_status_points)` is unique
 
 These rules are enforced both at the API level and in the database via constraints.
 
 ### How a customer's loyalty status is assigned
 
 - On customer creation, the backend assigns an initial `loyalty_status` using the tier rules with `status_points = 0`.
 - On point earn / rule actions, the backend recomputes the status based on `status_points`:
   - Only tiers with `active = true` are eligible.
   - The chosen tier is the one with the greatest `min_status_points` such that `min_status_points <= status_points`.
 
 ### Active flag
 
 - A tier with `active = false` is **never selected** by the tier computation.
 - If a tier is deactivated, existing customers may still have its `key` until a recompute occurs.
 
 ### After changing tiers: recompute customers
 
 Changing tiers (create/update/delete/deactivate) does **not** automatically update all customers.
 
 To bring customers back in sync, call:
 
 - `POST /admin/loyalty-tiers/recompute-customers`
   - Header: `X-Brand: <brand>`
   - Effect: recomputes `Customer.loyalty_status` for all customers of the active brand from their `status_points`.
 
 Recommended usage (frontend/admin UI):
 
 - After saving tier changes, display a CTA/button: **"Recompute customers"**.
 - Or trigger recompute automatically after a successful tier update if your dataset is small.
 
 ## Internal job idempotence
 
 Internal jobs are **idempotent per (job, customer, bucket)**.
 
 - Each emitted event ID is deterministic:
   - `job_{job.id}_{bucket_key}_{brand}_{profileId}`
 - `bucket_key` is based on:
   - the date when there is no schedule
   - or the previous scheduled cron occurrence start instant (in UTC) when `schedule` is set (`type="cron"`), taking `timezone` into account
 
 Result:
 
 - Running the same job twice in the **same bucket** yields:
   - `created = 0`
   - `idempotentExisting > 0`
 - Running the job in a **different bucket** yields different event IDs and can create new events for the same customers.
 
 ## Quick start (minimum setup)
 
 This is the minimal setup to see the loyalty engine working end-to-end for a brand.
 
 ### 1) Create an EXTERNAL event type (e.g. PURCHASE)
 
 The Rule Engine requires the referenced `transaction_type` to exist and be active.
 
 ### 2) Create a rule on this event type
 
 Example: earn points based on the payload amount.
 
 ### 3) Ingest a transaction
 
 Ingestion is multi-brand: the `brand` is in the request body (not `X-Brand`).
 
 ### 4) Read customer wallet / loyalty
 
 Use brand-scoped reads via `X-Brand`.
 
 ## PowerShell examples
 
 Use `ConvertTo-Json -Depth 20` (or higher) to avoid truncated output.
 
 ### 0) Create an EXTERNAL EventType + Rule + ingest a transaction
 
 ```powershell
 # Create EXTERNAL event type
 Invoke-RestMethod -Method Post "http://127.0.0.1:8000/admin/event-types" `
   -Headers @{ "Content-Type"="application/json"; "X-Brand"="BRAND_A" } `
   -Body '{
     "key": "PURCHASE",
     "origin": "EXTERNAL",
     "name": "Purchase",
     "description": "Customer purchase",
     "active": true,
     "payload_schema": null
   }'
 
 # Create a rule to earn points from amount
 Invoke-RestMethod -Method Post "http://127.0.0.1:8000/rules" `
   -Headers @{ "Content-Type"="application/json"; "X-Brand"="BRAND_A" } `
   -Body '{
     "transaction_type": "PURCHASE",
     "priority": 0,
     "active": true,
     "conditions": null,
     "actions": [
       { "type": "earn_points_from_amount", "rate": 1.0, "amount_path": "amount" }
     ]
   }'
 
 # Ingest a transaction (multi-brand: brand is in the body)
 Invoke-RestMethod -Method Post "http://127.0.0.1:8000/transactions" `
   -Headers @{ "Content-Type"="application/json" } `
   -Body '{
     "brand": "BRAND_A",
     "profileId": "p-1",
     "eventType": "PURCHASE",
     "eventId": "purchase_001",
     "source": "WEB",
     "payload": { "amount": 120 }
   }'
 
 # Read wallet (brand-scoped)
 Invoke-RestMethod -Method Get "http://127.0.0.1:8000/wallet/BRAND_A/p-1" `
   -Headers @{ "X-Brand"="BRAND_A" } |
   ConvertTo-Json -Depth 10
 ```
 
 ### 1) Create an INTERNAL EventType
 
 ```powershell
 Invoke-RestMethod -Method Post "http://127.0.0.1:8000/admin/event-types" `
   -Headers @{ "Content-Type"="application/json"; "X-Brand"="BRAND_A" } `
   -Body '{
     "key": "JOB_PING",
     "origin": "INTERNAL",
     "name": "Job ping",
     "description": "Internal event emitted by Internal Jobs",
     "active": true,
     "payload_schema": null
   }'
 ```
 
 ### 2) Create an internal job
 
 ```powershell
 $job = Invoke-RestMethod -Method Post "http://127.0.0.1:8000/admin/internal-jobs" `
   -Headers @{ "Content-Type"="application/json"; "X-Brand"="BRAND_A" } `
   -Body '{
     "job_key": "PING_ACTIVE",
     "transaction_type": "JOB_PING",
     "selector": {"field":"customer.status","operator":"in","value":["ACTIVE"]},
     "payload_template": { "hello": "world" },
     "active": true,
     "schedule": { "type": "cron", "cron": "*/2 * * * *" }
   }'
 
 $job.id
 ```
 
 ### 3) Preview internal job
 
 ```powershell
 Invoke-RestMethod -Method Post "http://127.0.0.1:8000/admin/internal-jobs/$($job.id)/preview?limit=10" `
   -Headers @{ "X-Brand"="BRAND_A" } |
   ConvertTo-Json -Depth 10
 ```
 
 ### 4) Run internal job twice (idempotence)
 
 ```powershell
 Invoke-RestMethod -Method Post "http://127.0.0.1:8000/admin/internal-jobs/$($job.id)/run" `
   -Headers @{ "X-Brand"="BRAND_A" } |
   ConvertTo-Json -Depth 10
 
 Invoke-RestMethod -Method Post "http://127.0.0.1:8000/admin/internal-jobs/$($job.id)/run" `
   -Headers @{ "X-Brand"="BRAND_A" } |
   ConvertTo-Json -Depth 10
 ```
 
 ### 5) Fetch UI bundles
 
 ```powershell
 Invoke-RestMethod -Method Get "http://127.0.0.1:8000/admin/rules/ui-bundle" `
   -Headers @{ "X-Brand"="BRAND_A" } |
   ConvertTo-Json -Depth 30
 
 Invoke-RestMethod -Method Get "http://127.0.0.1:8000/admin/internal-jobs/ui-bundle" `
   -Headers @{ "X-Brand"="BRAND_A" } |
   ConvertTo-Json -Depth 30
 ```
 
 ## API overview
 
 This section lists the main HTTP routes exposed by the backend (see `app/main.py`).
 
 ### Public / integration endpoints
 
 - `POST /transactions`
   - Ingest an event (`EventCreate`) and processes rules/actions.
   - Multi-brand ingestion: the payload contains `brand`.
 
 - `GET /transactions`
   - Brand-scoped listing (via `X-Brand`).
 
 - `GET /transactions/{transaction_id}`
 - `GET /transactions/{transaction_id}/executions`
 
 - `POST /customers/upsert`
   - Creates or updates a customer profile (brand-scoped via `X-Brand`).
 
 - `GET /customers/{brand}/{profile_id}`
 - `GET /customers/{brand}/{profile_id}/wallet`
 - `GET /customers/{brand}/{profile_id}/point-movements`
 - `GET /customers/{brand}/{profile_id}/rewards`
 - `POST /customers/{brand}/{profile_id}/rewards/{customer_reward_id}/use`
 - `GET /customers/{brand}/{profile_id}/coupons`
 - `POST /customers/{brand}/{profile_id}/coupons/{customer_coupon_id}/use`
 - `PATCH /customers/{brand}/{profile_id}/coupons/{customer_coupon_id}/use`
 - `POST /customers/{brand}/{profile_id}/coupons/{customer_coupon_id}/reopen`
 - `PATCH /customers/{brand}/{profile_id}/coupons/{customer_coupon_id}/reopen`
 - `GET /customers/{brand}/{profile_id}/loyalty`
 - `POST /customers/{brand}/{profile_id}/loyalty/set-tier`
 - `PATCH /customers/{brand}/{profile_id}/loyalty/set-tier`
 
 - `GET /wallet/{brand}/{profile_id}`
   - Returns points balance for a customer.
 
 - `POST /imports/customers`
   - Import customers via CSV.
   - Note: customer rows carry `brand`.
 
 - `POST /imports/events`
   - Import events via CSV (pre-checks that customers exist, then ingests events).
 
 ### Admin endpoints (brand-scoped)
 
 - **Event Types**: `GET/POST/PATCH/DELETE /admin/event-types`
   - `origin` can be `EXTERNAL` (ingested) or `INTERNAL` (emitted by jobs).
 
 - **Rules**: `GET/POST/PATCH/DELETE /rules`
   - Requires the referenced `transaction_type` to exist and be active.
   - Segment targeting is supported via `segment_ids`.
 
 - **Rewards**: `GET/POST/PATCH/DELETE /rewards`
 
 - **Loyalty tiers**: `GET/POST/PATCH/DELETE /admin/loyalty-tiers`
   - Used to compute the customer loyalty status and next tier information.
 

 - **Bonus definitions**: `GET/POST/PATCH/DELETE /admin/bonus-definitions`
   - Includes `GET /admin/bonus-definitions/ui-catalog`.

 - **Bonus awards**: `GET /admin/bonus-awards` (read-only list) and `GET /admin/bonus-awards/{id}`

 - **Internal jobs**: `GET/POST/PATCH/DELETE /admin/internal-jobs`
   - Includes:
     - `POST /admin/internal-jobs/{job_id}/preview`
     - `POST /admin/internal-jobs/{job_id}/run`
   - Segment targeting is supported via `segment_id` (top-level).

 - **UI options helpers**:
   - `GET /admin/ui-options/rewards`
   - `GET /admin/ui-options/event-types`
   - `GET /admin/ui-options/loyalty-tiers`
   - `GET /admin/ui-options/customer-tags`
   - `GET /admin/ui-options/product-categories`
   - `GET /admin/ui-options/products`
   - `GET /admin/ui-options/segments`

 - **Admin maintenance**:
   - `POST /admin/rewards/expire`

 ## Product Catalog

 The Product Catalog is a brand-scoped reference table used for:

 - Defining products and their points value.
 - Attaching products to Rewards (reward bundles).
 - Computing earned points from purchased products in transaction payloads (rule engine helper function).

 All admin endpoints are brand-scoped via `X-Brand`.

 ### Product Categories CRUD

 - `GET /admin/product-categories`
 - `POST /admin/product-categories`
 - `GET /admin/product-categories/{category_id}`
 - `PATCH /admin/product-categories/{category_id}`
 - `DELETE /admin/product-categories/{category_id}`

 UI helper:

 - `GET /admin/ui-options/product-categories`

 ### Products CRUD

 - `GET /admin/products`
   - Supports filtering by `category_id`.
 - `POST /admin/products`
 - `GET /admin/products/{product_id}`
 - `PATCH /admin/products/{product_id}`
 - `DELETE /admin/products/{product_id}`

 UI helper:

 - `GET /admin/ui-options/products`

 ### Products attached to Rewards

 Rewards can embed a list of products during create/update. The backend persists the association in `reward_products`.

 Admin helper endpoint:

 - `GET /admin/products/by-reward/{reward_id}`

 ### Using products in Rules (earn points)

 The rule engine includes a function to compute points from Unomi-like payloads:

 - `$fn: sum_product_points_unomi`

 Expected payload shape (best-effort / tolerant):

 - `payload.productNames`: list of product identifiers (strings)
 - `payload.productQuantities`: list of quantities (numbers)

 The function:

 - normalizes each product name to match `Product.match_key`
 - multiplies `Product.points_value * quantity`
 - ignores unknown products (does not fail rule evaluation)

 Example rule action (conceptual):

 - action `earn_points` with an expression using `$fn: sum_product_points_unomi`

 ## Segmentation

 Segments are brand-scoped customer groups used for targeting:

 - Rules (rule only applies if the customer is in one of the segments)
 - Internal Jobs (job processes only customers in the segment)

 There are two types:

 - **Dynamic segments**: defined by an AST condition block (same structure as rule conditions). Membership is recomputed by a maintenance job.
 - **Static segments**: membership is managed manually by adding/removing customers.

 ### Segments CRUD

 - `GET /admin/segments`
 - `POST /admin/segments`
 - `GET /admin/segments/{segment_id}`
 - `PATCH /admin/segments/{segment_id}`
 - `DELETE /admin/segments/{segment_id}`

 UI helper:

 - `GET /admin/ui-options/segments`

 ### Static segment membership

 - `GET /admin/segments/{segment_id}/members`
 - `POST /admin/segments/{segment_id}/members`
   - Only allowed for `is_dynamic = false`.
 - `DELETE /admin/segments/{segment_id}/members/{customer_id}`
   - Only allowed for `is_dynamic = false`.

 ### Dynamic segment recomputation

 Dynamic segment membership is recomputed by the system-managed internal job:

 - `MAINT_RECOMPUTE_SEGMENTS`

 The worker process (`python -m app.services.internal_job_scheduler`) must be running for automatic daily recomputation.

 ### Using segments in Rules

 Rules support segment targeting via:

 - `Rule.segment_ids: [UUID, ...]`

 Semantics:

 - If `segment_ids` is empty/null: rule behaves as before.
 - If `segment_ids` is set: customer must be a member of at least one segment to evaluate/apply the rule.
 - If both `segment_ids` and `conditions` are present: membership check happens first, then `conditions` are evaluated.

 ### Using segments in Internal Jobs

 Internal Jobs support segment targeting via:

 - `InternalJob.segment_id: UUID | null`

 Semantics:

 - If `segment_id` is null: job uses `selector` only.
 - If `segment_id` is set: job targets customers in the segment, then applies `selector` (AST) as an additional filter.

 ### Campaigns (legacy)

 There is a legacy campaigns router at `GET/POST/PATCH/DELETE /campaigns`. This feature is slated for removal in favor of Rules + Internal Jobs.
