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
 
 ## Running the Internal Job scheduler (cron worker)
 
 Internal Jobs are executed automatically by a separate scheduler loop. In production, you typically run **two processes** on the same server:
 
 - API: `uvicorn app.main:app --host 0.0.0.0 --port 8000`
 - Scheduler worker: `python -m app.services.internal_job_scheduler`
 
 If the worker is not running, jobs will **not** execute automatically (you can still use `POST /admin/internal-jobs/{job_id}/run`).
 
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
 
 The Rule Engine requires the referenced `event_type` to exist and be active.
 
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
     "event_type": "PURCHASE",
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
     "event_type": "JOB_PING",
     "selector": { "status_in": ["ACTIVE"] },
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
 - `GET /customers/{brand}/{profile_id}/loyalty`

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
   - Requires the referenced `event_type` to exist and be active.

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

 - **UI options helpers**:
   - `GET /admin/ui-options/rewards`
   - `GET /admin/ui-options/event-types`
   - `GET /admin/ui-options/loyalty-tiers`
   - `GET /admin/ui-options/customer-tags`

 - **Admin maintenance**:
   - `POST /admin/rewards/expire`

 ### Campaigns (legacy)

 There is a legacy campaigns router at `GET/POST/PATCH/DELETE /campaigns`. This feature is slated for removal in favor of Rules + Internal Jobs.
