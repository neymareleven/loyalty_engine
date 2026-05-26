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

 Optional (segmentation Unomi — see `.env.example`) :

- `UNOMI_BASE_URL`, `UNOMI_USERNAME`, `UNOMI_PASSWORD` — suffisent pour **toutes** les marques
- Marque courante : toujours `X-Brand` / `?brand=` (rien à lister dans le `.env`)
- Optionnel : `UNOMI_INTERNAL_BRANDS` (exclusions) ou `UNOMI_BRANDS` (opt-in restreint)

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

## Coupon types, rewards, and issuing coupons from rules

Rules can issue coupons to customers via the `issue_coupon` action.

### Data model

```
CouponType ◄──── coupon_type_rewards ────► Reward
```

- **`coupon_type_rewards`**: seule source de vérité — quelles récompenses sont offertes par un type de coupon.
- **Ordre métier recommandé** : créer le **type de coupon** d’abord, puis les **rewards** rattachées via `coupon_type_ids`.
- **UI / hints** : `GET /admin/coupon-types/ui-catalog`, `GET /admin/rewards/ui-catalog`, `GET /admin/customer-entitlements/ui-catalog`.

### Admin workflow (catalogue vs client)

**Catalogue (configuration)**

1. `POST /admin/coupon-types` — sans rewards.
2. `POST /rewards` avec `coupon_type_ids` (obligatoire).
3. Liens optionnels / réordonnancement : `PUT /admin/coupon-types/{id}/rewards` (préféré) plutôt que `PATCH /rewards/{id}` avec `coupon_type_ids` (exception).
4. Règles : action `issue_coupon` ; rewards disponibles via `GET /admin/ui-options/coupon-types/{id}/rewards`.

**Suppression catalogue**

| Ressource | Condition DELETE | Sinon |
|-----------|------------------|--------|
| Coupon type | `can_delete` / `customer_coupon_count == 0` | `PATCH` `active: false` |
| Reward | Aucune `CustomerReward` `USED`/`EXPIRED` | `PATCH` `active: false` |

**Client (émissions — pas de DELETE)**

- Coupon client : `PATCH …/coupons/{id}/status` (`ISSUED` \| `USED` \| `EXPIRED`) — pas de suppression.
- Rewards client : historique conservé ; libellés depuis `payload.rewardSnapshot` / `payload.couponTypeSnapshot`.

### 1) Créer un type de coupon

```json
POST /admin/coupon-types
{
  "name": "Coupon anniversaire",
  "description": "Offre anniversaire",
  "validity_days": 365,
  "active": true
}
```

Aucune récompense à ce stade.

### 2) Créer une récompense (rattachée au coupon)

```json
POST /rewards
{
  "name": "Bon -10%",
  "description": "...",
  "active": true,
  "coupon_type_ids": ["<coupon-type-uuid>"],
  "products": []
}
```

`coupon_type_ids` est **obligatoire** (au moins un type de coupon existant).

### 3) Règle `issue_coupon`

- **`coupon_type_id`** (required)
- **`frequency`** (optional): `ONCE_PER_CUSTOMER`, `ONCE_PER_CALENDAR_YEAR`, `ALWAYS`
- **`reward_ids`** (optional):
  - **omis** (`null`) → toutes les rewards actives liées au type de coupon
  - **liste** → **strict** : uniquement ces IDs (doivent être liés au coupon type et actifs) ; erreur 400 si ID invalide
  - **`[]`** → coupon sans rewards client

```json
{
  "type": "issue_coupon",
  "coupon_type_id": "<coupon-type-uuid>",
  "frequency": "ONCE_PER_CALENDAR_YEAR",
  "reward_ids": ["<reward-uuid-1>"]
}
```

### Endpoints utiles

- `GET /admin/coupon-types/{id}/rewards` — rewards liées au coupon
- `PUT /admin/coupon-types/{id}/rewards` — remplacer les liens `{ "reward_ids": [...] }` (réordonnancement admin)
- `GET /rewards?coupon_type_id=<uuid>` — filtrer les rewards d’un coupon
- `GET /admin/ui-options/coupon-types` — liste pour formulaires
- `GET /admin/ui-options/coupon-types/{coupon_type_id}/rewards` — choix pour `issue_coupon.reward_ids`

Coupon type API metadata (`CouponTypeOut`):

- `customer_coupon_count` — number of issued customer coupons for this type
- `can_delete` — `true` only when `customer_coupon_count == 0`
- `recommended_action` — `"deactivate"` when delete is blocked; `null` when delete is allowed

Customer reward snapshot (on `issue_coupon` / `issue_reward`):

- `CustomerReward.payload` includes `rewardSnapshot` and, when issued with a coupon, `couponTypeSnapshot` (plus legacy `name` / `description` / `rewardId` keys).

Reward deletion behavior:

- `DELETE /rewards/{reward_id}` is **blocked (409)** if any `CustomerReward` is `USED` or `EXPIRED` for that reward (`recommendedAction: deactivate`).
- Otherwise, any `ISSUED` entitlements are marked `CANCELLED` before delete; response includes `cancelled_count`.

### Backfill snapshots (historique)

Migration `fe67ab89cd01` remplit `rewardSnapshot` / `couponTypeSnapshot` sur les `customer_rewards` existants (à partir de `rewards`, `coupon_types`, ou champs legacy `payload.name` / `payload.couponType`).

```bash
alembic upgrade head
```

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
       { "type": "earn_points", "points": { "$path": "amount" } }
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
 - `GET /customers/{brand}/{profile_id}/coupons-with-rewards`
 - `PATCH /customers/{brand}/{profile_id}/coupons/{customer_coupon_id}/status` — `{ "status": "ISSUED" | "USED" | "EXPIRED" }` (coupon + rewards liées)
 - `GET /customers/{brand}/{profile_id}/loyalty`
 - `GET /customers/{brand}/{profile_id}/loyalty/history` (inclut `ADMIN_SET_TIER`)
 - `PATCH /customers/{brand}/{profile_id}/loyalty/status` — `{ "tierKey": "GOLD", "reason": "optional" }` (ajuste points + palier, réponse avec `loyaltyOverride`)
 
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
   - `GET /admin/rewards/ui-catalog` (workflow + politique delete)
 
 - **Loyalty tiers**: `GET/POST/PATCH/DELETE /admin/loyalty-tiers`
   - Used to compute the customer loyalty status and next tier information.
 

 - **Coupon types**: `GET/POST/PATCH/DELETE /admin/coupon-types`
   - `GET /admin/coupon-types/ui-catalog` (workflow + politique delete)
   - `GET /admin/coupon-types/{id}/rewards`
   - `PUT /admin/coupon-types/{id}/rewards`

 - **Customer entitlements (UI)**: `GET /admin/customer-entitlements/ui-catalog` (statuts coupon client, pas de DELETE)

 - **Internal jobs**: `GET/POST/PATCH/DELETE /admin/internal-jobs`
   - Includes:
     - `POST /admin/internal-jobs/{job_id}/preview`
     - `POST /admin/internal-jobs/{job_id}/run`
   - Segment targeting is supported via `segment_id` (top-level).

 - **UI options helpers**:
   - `GET /admin/ui-options/rewards`
   - `GET /admin/ui-options/coupon-types`
   - `GET /admin/ui-options/coupon-types/{coupon_type_id}/rewards`
   - `GET /admin/ui-options/event-types`
   - `GET /admin/ui-options/loyalty-tiers`
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

 **Guide intégration frontend (INTERNAL + Unomi)** : [docs/SEGMENTS_FRONTEND.md](docs/SEGMENTS_FRONTEND.md) — détail Unomi statique (membres + suppression) : [section dédiée](docs/SEGMENTS_FRONTEND.md#unomi--segment-statique--membres-et-suppression)

 Segments are brand-scoped customer groups used for targeting:

 ### Modes: INTERNAL vs Unomi (priorité CDP)

 | Mode | Quand | Source de vérité membership |
 |------|--------|------------------------------|
 | **INTERNAL** | Marque sans Unomi (`segmentation_mode=INTERNAL`) | Table `segment_members` dans le moteur |
 | **UNOMI** | Marque avec instance Unomi (`segmentation_mode=UNOMI` + URL/credentials) | Apache Unomi ; le moteur garde un **registre** (`segments.id` UUID) pour rules/jobs |

 Configuration **uniquement via `.env`** (pas de credentials Unomi en base) :

 ```env
 UNOMI_BASE_URL=https://cdp.example.com:9443
 UNOMI_USERNAME=karaf
 UNOMI_PASSWORD=karaf
 # Pas besoin de UNOMI_BRANDS si toutes les marques partagent ce CDP.
 # Exceptions seulement (liste courte) :
 # UNOMI_INTERNAL_BRANDS=demo_sandbox
 ```

 La marque active vient du **compte utilisateur** via `X-Brand` à chaque appel — pas d’une liste statique de 100 marques dans le `.env`. Le scope Unomi par défaut = clé de cette marque.

 Vérification : `GET /admin/segments/segmentation-mode` + `X-Brand: <marque>` → `unomiPolicy: "all_brands"`, `activeBrand`, `currentBrandUsesUnomi`.

 **Segments manuels Unomi** : pas de table `segment_members` ; chaque ajout pousse le `profileId` Unomi du client dans `manual_profile_ids` et reconstruit la condition Unomi :

 ```text
 OR( itemId = profileId_1, itemId = profileId_2, … )
 ```

 C'est le pattern recommandé quand Unomi ne permet pas d'« épingler » un profil sans condition. `POST …/members` et bulk appellent cette synchro automatiquement.

 **Segments dynamiques Unomi** : la condition est stockée côté Unomi (`unomi_condition`) ; le recalcul membership est fait par Unomi (pas `MAINT_RECOMPUTE_SEGMENTS`).

 - `GET /admin/segments/segmentation-mode` — mode actif + connectivité Unomi
 - `POST /admin/segments/{id}/sync-unomi` — repousse la liste manuelle vers Unomi

 Règles et jobs continuent de référencer le **UUID** du registre moteur ; l'appartenance est résolue via Unomi (`/segments/{id}/match/{profileId}`) ou la liste manuelle.

 - Rules (rule only applies if the customer is in one of the segments)
 - Internal Jobs (job processes only customers in the segment)

 There are two types:

 - **Dynamic segments**: defined by an AST condition block (same structure as rule conditions). Membership is recomputed by a maintenance job.
 - **Static segments**: membership is managed manually by adding/removing customers.

 ### Segments CRUD

 - `GET /admin/segments` — liste enrichie (`member_count`, `needs_recompute`, `can_delete`, …)
 - `POST /admin/segments` — recalcul auto des segments dynamiques (`?recompute=true` par défaut)
 - `GET /admin/segments/{segment_id}`
 - `PATCH /admin/segments/{segment_id}` — transition `is_dynamic` avec nettoyage membres ; recalcul auto si conditions / activation changent
 - `DELETE /admin/segments/{segment_id}` — **409** si le segment est référencé par des règles (`segment_ids`) ou des jobs (`segment_id`)
 - `POST /admin/segments/recompute` — recalcul manuel de tous les segments dynamiques actifs de la marque
 - `POST /admin/segments/{segment_id}/recompute` — recalcul d’un segment dynamique

 UI helpers:

 - `GET /admin/ui-options/segments` — liste simple pour sélecteurs
 - `GET /admin/segments/ui-catalog` — workflow admin (create/update/delete/recompute)
 - `GET /admin/segments/ui-options/condition-fields` — champs AST **client-only** (`customer.*`, `customer.metrics.*`, `system.*` ; pas de `payload.*`)

 ### Static segment membership

 - `GET /admin/segments/{segment_id}/members`
 - `POST /admin/segments/{segment_id}/members`
   - Only allowed for `is_dynamic = false`.
 - `DELETE /admin/segments/{segment_id}/members/{customer_id}`
   - Only allowed for `is_dynamic = false`.

Bulk helpers (recommended for multi-select UI):

- `POST /admin/segments/{segment_id}/members/bulk`
  - Adds one or more customers to a static segment.
  - Best-effort: returns a per-batch summary and continues on partial failures.
- `POST /admin/segments/{segment_id}/members/bulk-delete`
  - Removes one or more customers from a static segment.
  - Uses `POST` (instead of `DELETE` with a JSON body) for compatibility with common HTTP clients/proxies.

### Dynamic segment recomputation

 Dynamic segment membership is recomputed by:

 - **Automatique** : à la création / mise à jour d’un segment dynamique (query `recompute`, défaut `true`)
 - **Manuel** : `POST /admin/segments/recompute` ou `POST /admin/segments/{segment_id}/recompute`
 - **Planifié** : job système `MAINT_RECOMPUTE_SEGMENTS` (cron quotidien par défaut)

 `SegmentOut.needs_recompute` est `true` si le segment est dynamique actif et que `updated_at` (ou l’absence de `last_computed_at`) indique un recalcul nécessaire.

 Les segments dynamiques n’acceptent **pas** de membres `STATIC` (ajout manuel refusé). Au recalcul, d’éventuels membres `STATIC` résiduels sont supprimés. Passage static→dynamic : supprimer les `STATIC` via `?clear_static_on_dynamic=true` ou les retirer avant le PATCH.

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

