 # Loyalty Engine
 
 Backend API for a multi-brand loyalty engine.
 
 Key features:
 
 - **Brand scoping** for admin operations via `X-Brand`.
 - **Multi-brand ingestion** for transactions/events (ingestion payload carries `event.brand`).
 - **Form-friendly admin APIs** for building Rules and Internal Jobs without manual JSON typing.
 
 ## Brand scoping model
 
 - **Admin routes** (rules, rewards, event types, tiers, internal jobs, UI options) are scoped by the active brand.
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
   - Includes:
     - rule schemas + UI hints
     - rule action catalog
     - rule condition catalog
     - UI options (rewards, event types, loyalty tiers, customer tags)
 
 ### Internal Jobs
 
 - `GET /admin/internal-jobs/ui-catalog`
   - Returns:
     - `job`: JSON schema + UI hints for `InternalJobCreate`
     - `jobTypes`: UI presets to prefill job forms
     - `selector`: selector builder catalog
     - `payloadTemplate`: UI hints for payload template editing
 
 - `GET /admin/internal-jobs/ui-bundle`
   - Single-call bundle for the Internal Job Builder.
   - Includes:
     - `uiCatalog`
     - `uiOptions.eventTypes` (INTERNAL)
     - `uiOptions.loyaltyTiers`
     - `uiOptions.customerTags`
 
 - `POST /admin/internal-jobs/{job_id}/preview`
   - Preview selected customers (count + sample).
 
 - `POST /admin/internal-jobs/{job_id}/run`
   - Runs the job once and emits INTERNAL transactions.
 
 ## Internal job idempotence
 
 Internal jobs are **idempotent per (job, customer, bucket)**.
 
 - Each emitted event ID is deterministic:
   - `job_{job.id}_{bucket_key}_{brand}_{profileId}`
 - `bucket_key` is based on:
   - the date when there is no schedule
   - or `floor(now / scheduleSeconds)` when `schedule` is set
 
 Result:
 
 - Running the same job twice in the **same bucket** yields:
   - `created = 0`
   - `idempotentExisting > 0`
 - Running the job in a **different bucket** yields different event IDs and can create new events for the same customers.
 
 ## PowerShell examples
 
 Use `ConvertTo-Json -Depth 20` (or higher) to avoid truncated output.
 
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
     "schedule": "3600"
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
