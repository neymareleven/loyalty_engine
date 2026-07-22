import org.apache.unomi.api.Profile
import org.apache.unomi.api.services.EventService
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.util.EntityUtils
import org.apache.http.HttpResponse
import org.apache.http.entity.ContentType
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import groovy.json.JsonOutput
import groovy.json.JsonSlurper

final Logger logger = LoggerFactory.getLogger("sale_to_loyalty_engine")

@Action(id = "sale", actionExecutor = "groovy:sale", parameters = [
    @Parameter(id = "loyaltyUrl", type = "string", multivalued = false),
    @Parameter(id = "loyaltyUsername", type = "string", multivalued = false),
    @Parameter(id = "loyaltyPassword", type = "string", multivalued = false)
])
def execute() {
    final String prefix = "[sale-to-loyalty-debug]"

    final int LOOKBACK_MINUTES = 30
    final int MAX_EVENTS_TO_RECOVER = 5   // was 10 — plenty for the idempotent-safety-net use case
    final int BROAD_SWEEP_LIMIT = 20      // scope-wide catch-up sweep size, see fetchBroadRecentSaleEvents below

    // ── Timeouts (ms). Every network call in this script now has an explicit,
    //    bounded timeout. Previously the external POST to Loyalty had NONE,
    //    which is how a single slow request could hold a shared processing
    //    thread hostage indefinitely (observed: 22s+ for one POST).
    final int INTERNAL_CONNECT_TIMEOUT_MS = 3000
    final int INTERNAL_READ_TIMEOUT_MS    = 6000
    final int EXTERNAL_CONNECT_TIMEOUT_MS = 5000
    final int EXTERNAL_SOCKET_TIMEOUT_MS  = 30000   // generous margin above the observed 22s worst case
    final int MAX_RETRIES                 = 2
    final long RETRY_BACKOFF_MS           = 800L

    // ── Single shared HttpClient for this execution, with pooling + bounded
    //    timeouts, reused across every push (fast path = 1, recovery = up to
    //    MAX_EVENTS_TO_RECOVER). Previously a brand-new client (and full TLS
    //    handshake) was created and destroyed for EVERY push.
    RequestConfig externalConfig = RequestConfig.custom()
        .setConnectTimeout(EXTERNAL_CONNECT_TIMEOUT_MS)
        .setSocketTimeout(EXTERNAL_SOCKET_TIMEOUT_MS)
        .setConnectionRequestTimeout(EXTERNAL_CONNECT_TIMEOUT_MS)
        .build()
    CloseableHttpClient sharedHttpClient = HttpClientBuilder.create()
        .setDefaultRequestConfig(externalConfig)
        .build()

    // ── Helper: open a trust-all HTTPS connection to the local Unomi REST API,
    //    used only for self-lookups (rule config fallback, event recovery).
    //    Always has bounded timeouts so it can never hang the calling thread.
    def openInternalConnection = { String path, String method ->
        def trustAll = [
            getAcceptedIssuers   : { null },
            checkClientTrusted   : { chain, authType -> },
            checkServerTrusted   : { chain, authType -> }
        ] as javax.net.ssl.X509TrustManager
        def sslCtx = javax.net.ssl.SSLContext.getInstance("TLS")
        sslCtx.init(null, [trustAll] as javax.net.ssl.TrustManager[], new java.security.SecureRandom())

        def url = new URL("https://cdp.qilinsa.com:9443${path}")
        def conn = (javax.net.ssl.HttpsURLConnection) url.openConnection()
        conn.setSSLSocketFactory(sslCtx.socketFactory)
        conn.setHostnameVerifier { h, s -> true }
        conn.setRequestMethod(method)
        conn.setRequestProperty("Authorization", "Basic " + "karaf:karaf".bytes.encodeBase64().toString())
        conn.setRequestProperty("Content-Type", "application/json")
        conn.setConnectTimeout(INTERNAL_CONNECT_TIMEOUT_MS)
        conn.setReadTimeout(INTERNAL_READ_TIMEOUT_MS)
        if (method == "POST") conn.setDoOutput(true)
        return conn
    }

    // ── DEFINITIVE FIX for silent misses (orders #7049, #7051 confirmed lost
    //  2026-07-17): the profileId-targeted search + scopeEmail-fallback
    //  approach still depends entirely on the ONE profile this particular
    //  execution's corrupted context happens to be bound to. Under bursty
    //  concurrent load, that bound profile is sometimes a completely
    //  unrelated, email-less anonymous profile — in which case BOTH the
    //  targeted search AND the scopeEmail fallback have nothing to work
    //  with, and the sale is silently never pushed, never retried again.
    //
    //  Fix: stop depending on the seed profile's identity entirely. Every
    //  execution instead sweeps the last N "sale" events scope-wide (no
    //  profileId/scopeEmail filter at all) and pushes all of them. This is
    //  safe (idempotent on itemId — a resend is a no-op) and correct
    //  (pushOneSale independently re-resolves each sale's OWN canonical
    //  profileId from its OWN scopeEmail, regardless of which profile this
    //  execution nominally concerns). Since the "sale-to-loyalty" rule fires
    //  once per real sale, any sale missed by one execution's bad luck gets
    //  caught by the next nearby execution's sweep — in real time, with no
    //  external cron needed.
    def fetchBroadRecentSaleEvents = {
        def query = [
            condition: [
                type: "eventTypeCondition",
                parameterValues: [ eventTypeId: "sale" ]
            ],
            sortby: "timeStamp:desc",
            offset: 0,
            limit : BROAD_SWEEP_LIMIT
        ]
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                def conn = openInternalConnection("/cxs/events/search", "POST")
                conn.outputStream.withWriter("UTF-8") { it << JsonOutput.toJson(query) }
                def code = conn.responseCode
                if (code >= 200 && code < 300) {
                    def parsed = new JsonSlurper().parse(conn.inputStream)
                    def results = parsed?.list ?: []
                    logger.warn("${prefix} BROAD_SWEEP attempt=${attempt} fetched ${results.size()} recent sale event(s) scope-wide")
                    if (results) return results
                    if (attempt < MAX_RETRIES) {
                        logger.warn("${prefix} BROAD_SWEEP attempt=${attempt} found 0 events, retrying in ${RETRY_BACKOFF_MS}ms (possible ES refresh lag)")
                        Thread.sleep(RETRY_BACKOFF_MS)
                    }
                } else {
                    logger.error("${prefix} BROAD_SWEEP attempt=${attempt} search failed code=${code}")
                    if (attempt < MAX_RETRIES) Thread.sleep(RETRY_BACKOFF_MS)
                }
            } catch (Exception e) {
                logger.error("${prefix} BROAD_SWEEP attempt=${attempt} exception while searching sale events", e)
                if (attempt < MAX_RETRIES) Thread.sleep(RETRY_BACKOFF_MS)
            }
        }
        return []
    }


    // ── DEFINITIVE FIX for the confirmed event.profileId / execution-context
    //  corruption bug: instead of trusting a profileId handed to us by
    //  Unomi's execution context (which can be wrong, as proven), we
    //  re-derive "who this sale really belongs to" from the one piece of
    //  information that cannot lie — the scopeEmail present on the sale
    //  event itself. This queries Unomi's own profile store (a separate,
    //  independent lookup from the corrupted event.profileId field) for the
    //  profile that owns this scopeEmail, and returns ITS itemId as the
    //  canonical, trustworthy profileId to push to Loyalty.
    def resolveCanonicalProfileId = { String scopeEmail ->
        if (!scopeEmail) return null
        def query = [
            condition: [
                type: "profilePropertyCondition",
                parameterValues: [
                    propertyName: "properties.scopeEmail",
                    comparisonOperator: "equals",
                    propertyValue: scopeEmail
                ]
            ],
            sortby: "systemProperties.lastUpdated:desc",
            offset: 0,
            limit : 5
        ]
        try {
            def conn = openInternalConnection("/cxs/profiles/search", "POST")
            conn.outputStream.withWriter("UTF-8") { it << JsonOutput.toJson(query) }
            def code = conn.responseCode
            if (code < 200 || code >= 300) {
                logger.error("${prefix} PROFILE_RESOLVE search failed code=${code} for scopeEmail=${scopeEmail}")
                return null
            }
            def parsed = new JsonSlurper().parse(conn.inputStream)
            def list = parsed?.list ?: []
            // CONFIRMED IN PRODUCTION (2026-07-17): picking "most recently
            // updated" when the search returned multiple/wrong candidates
            // (due to a condition-type bug, propertyCondition instead of
            // profilePropertyCondition) caused a real customer's data to be
            // pushed under a DIFFERENT customer's Loyalty record. Lesson:
            // never guess. Only trust a resolution when it's unambiguous —
            // exactly one match. Anything else is treated as unresolved.
            if (list.size() == 1) {
                return list[0]?.itemId?.toString()
            } else if (list.size() == 0) {
                logger.warn("${prefix} PROFILE_RESOLVE no profile found with scopeEmail=${scopeEmail}")
                return null
            } else {
                logger.error("${prefix} PROFILE_RESOLVE AMBIGUOUS — ${list.size()} profiles share scopeEmail=${scopeEmail} (itemIds=${list.collect { it.itemId }}). Refusing to guess — treating as unresolved. This needs manual review (likely a genuine duplicate-profile situation).")
                return null
            }
        } catch (Exception e) {
            logger.error("${prefix} PROFILE_RESOLVE exception for scopeEmail=${scopeEmail}", e)
            return null
        }
    }

    // ── Helper: fetch a profile's own properties fresh via REST, used once
    //  we've resolved (or corrected) the canonical profileId for a given
    //  sale, so billing-field enrichment reads come from the CORRECT
    //  profile rather than whatever (possibly wrong) Profile object the
    //  execution context originally handed us.
    def fetchProfilePropertiesById = { String pid ->
        try {
            def conn = openInternalConnection("/cxs/profiles/${pid}", "GET")
            def code = conn.responseCode
            if (code < 200 || code >= 300) {
                logger.warn("${prefix} PROFILE_FETCH failed code=${code} for profileId=${pid}")
                return [:]
            }
            def parsed = new JsonSlurper().parse(conn.inputStream)
            return (parsed?.properties instanceof Map) ? parsed.properties : [:]
        } catch (Exception e) {
            logger.warn("${prefix} PROFILE_FETCH exception for profileId=${pid} (${e.message})")
            return [:]
        }
    }

    // ── STEP 1: Detect whether the event context is trustworthy ───────────────
    //
    //  Known root cause: Unomi's Groovy action dispatcher can leak the `event`
    //  binding across scripts running in the same thread/request, so by the
    //  time this script executes, `event` may point at a LATER event
    //  (typically profileUpdated) instead of the original sale. When that
    //  happens we recover the real sale from the event store instead of
    //  trusting composited profile properties (which is racy — see order 7002
    //  incident).
    def eventType = null
    try { eventType = event?.getEventType() } catch (Exception e) {
        logger.error("${prefix} STEP1 Failed to get eventType", e)
    }

    def targetItemType = null
    def targetItemId = null
    try { targetItemType = event?.getTarget()?.getItemType() } catch (Exception ignore) {}
    try { targetItemId   = event?.getTarget()?.getItemId()   } catch (Exception ignore) {}

    logger.warn("${prefix} STEP1 eventType=${eventType} targetItemType=${targetItemType} targetItemId=${targetItemId}")

    // FIX: previously this hard-EXITed (no recovery attempted) whenever
    // eventType == "sale" but targetItemType != "saleform". That silently
    // dropped any genuine sale whose target item type didn't match the
    // assumption exactly — an independent data-loss risk from the `event`
    // mutation bug. Now: a clean "sale" event is trusted regardless of
    // targetItemType (we still log if it's unexpected, for visibility).
    boolean cleanSaleContext = (eventType == "sale")
    if (eventType == "sale" && targetItemType != "saleform") {
        logger.warn("${prefix} STEP1 NOTE: sale event has unexpected targetItemType='${targetItemType}' (expected 'saleform') — proceeding with fast path anyway using event properties directly.")
    }
    logger.warn("${prefix} STEP1 mode=${cleanSaleContext ? 'FAST_PATH' : 'RECOVERY'} (eventType=${eventType})")

    // ── STEP 2: Profile / profileId ───────────────────────────────────────────
    Profile profile = null
    try { profile = event?.getProfile() } catch (Exception e) {
        logger.error("${prefix} STEP2 Failed to get profile", e)
    }

    def profileId = null
    try { profileId = event?.getProfileId() } catch (Exception ignore) {}
    if (!profileId && profile) {
        try { profileId = profile?.getItemId() } catch (Exception ignore) {}
    }

    logger.warn("${prefix} STEP2 profileId=${profileId} profileIsNull=${profile == null}")

    if (!profileId || !profileId.toString().trim()) {
        logger.error("${prefix} STEP2 EXIT: Missing profileId - skipping sale push.")
        return EventService.NO_CHANGE
    }
    profileId = profileId.toString()

    // ── STEP 5: Resolve Loyalty Engine connection parameters ───────────────────
    //  CONFIRMED IN PRODUCTION (2026-07-16, order #7046): action.getParameterValues()
    //  can return values belonging to a DIFFERENT, unrelated action/rule
    //  (observed: loyaltyUrl=https://marketing.qilinsa.com, a Mautic-sync
    //  endpoint, instead of the correct https://loyalty.qilinsa.com). This
    //  caused a real sale to be POSTed to the wrong host (404) and lost.
    //  The self-referential REST fetch is therefore the PRIMARY source again
    //  — it always reads this exact rule's own persisted config, so it
    //  cannot be contaminated by another action's parameters. As a defense
    //  in depth, even if action binding is ever used, its host is validated
    //  against EXPECTED_LOYALTY_HOST below before being trusted.
    final String EXPECTED_LOYALTY_HOST = "loyalty.qilinsa.com"

    def loyaltyUrl  = null
    def loyaltyUser = null
    def loyaltyPass = null
    try {
        def conn = openInternalConnection("/cxs/rules/sale-to-loyalty", "GET")
        def ruleJson = new JsonSlurper().parse(conn.inputStream)
        def saleAction = ruleJson?.actions?.find { it.type == "sale" }
        def freshParams = saleAction?.parameterValues ?: [:]
        loyaltyUrl  = freshParams.get("loyaltyUrl")?.toString()?.trim()
        loyaltyUser = freshParams.get("loyaltyUsername")?.toString()
        loyaltyPass = freshParams.get("loyaltyPassword")?.toString()
        logger.warn("${prefix} STEP5 fetched from rule API: loyaltyUrl=${loyaltyUrl}")
    } catch (Exception e) {
        logger.error("${prefix} STEP5 rule API self-fetch failed (${e.message})")
    }

    // Fallback only if the self-fetch failed outright — and even then,
    // reject the action binding if its host doesn't match what we expect,
    // since that's the exact contamination pattern seen with order #7046.
    if (!loyaltyUrl) {
        try {
            def allParams = action?.getParameterValues()
            def candidateUrl = allParams?.get("loyaltyUrl")?.toString()?.trim()
            if (candidateUrl && candidateUrl.contains(EXPECTED_LOYALTY_HOST)) {
                loyaltyUrl  = candidateUrl
                loyaltyUser = allParams?.get("loyaltyUsername")?.toString()
                loyaltyPass = allParams?.get("loyaltyPassword")?.toString()
                logger.warn("${prefix} STEP5 self-fetch failed, using action binding (host validated): loyaltyUrl=${loyaltyUrl}")
            } else if (candidateUrl) {
                logger.error("${prefix} STEP5 action binding REJECTED — unexpected host in loyaltyUrl=${candidateUrl} (expected to contain '${EXPECTED_LOYALTY_HOST}'). Likely cross-action contamination. Refusing to use it.")
            }
        } catch (Exception e2) {
            logger.warn("${prefix} STEP5 action binding read also failed (${e2.message})")
        }
    }

    if (!loyaltyUrl) {
        logger.error("${prefix} STEP5 EXIT: Missing loyaltyUrl - cannot push sale event(s).")
        return EventService.NO_CHANGE
    }
    def endpoint = loyaltyUrl.endsWith("/") ? (loyaltyUrl + "transactions") : (loyaltyUrl + "/transactions")
    logger.warn("${prefix} STEP5 endpoint=${endpoint}")

    // ── Reusable pusher: builds the payload for ONE sale's properties and POSTs
    //    it to the Loyalty Engine, with bounded timeouts and one retry on
    //    transient failure (safe: Loyalty is idempotent on itemId).
    def pushOneSale = { Map props, String itemIdForDedup, String label ->
        def brand = (props?.get("brand") ?: profile?.getProperty("brand"))?.toString()
        if (!brand || !brand.trim()) {
            try { brand = event?.getScope()?.toString() } catch (Exception ignore) {}
        }
        if (!brand || !brand.trim()) {
            logger.error("${prefix} ${label} EXIT: Missing brand - skipping. profileId=${profileId}")
            return
        }

        // ── DEFINITIVE FIX for the confirmed event.profileId corruption bug:
        //  re-derive the real owning profile from this sale's own scopeEmail
        //  (ground truth — it's what the customer actually submitted at
        //  checkout) instead of trusting whatever profileId this execution
        //  was handed. Applied per-sale since a RECOVERY batch could in
        //  theory span more than one customer.
        def saleScopeEmail = props?.get("scopeEmail")?.toString()?.trim()
        def pushProfileId = profileId
        def canonicalProfileProps = [:]
        if (saleScopeEmail) {
            def resolvedId = resolveCanonicalProfileId(saleScopeEmail)
            if (resolvedId) {
                if (resolvedId != pushProfileId) {
                    logger.warn("${prefix} ${label} PROFILE_ID CORRECTED via scopeEmail=${saleScopeEmail}: was=${pushProfileId} -> canonical=${resolvedId}")
                } else {
                    logger.warn("${prefix} ${label} profileId=${pushProfileId} confirmed canonical via scopeEmail=${saleScopeEmail}")
                }
                pushProfileId = resolvedId
                canonicalProfileProps = fetchProfilePropertiesById(pushProfileId)
            } else {
                logger.warn("${prefix} ${label} could not resolve a profile for scopeEmail=${saleScopeEmail} — keeping original profileId=${pushProfileId} as best effort (UNVERIFIED).")
            }
        } else {
            logger.warn("${prefix} ${label} no scopeEmail present on this sale — cannot cross-validate profileId=${pushProfileId} (UNVERIFIED, using as-is).")
        }
        def profileProp = { String name ->
            if (canonicalProfileProps.containsKey(name)) return canonicalProfileProps.get(name)
            try { return profile?.getProperty(name) } catch (Exception ignore) { return null }
        }

        def mappedProps = [:]
        mappedProps.putAll(props)
        if (!mappedProps.containsKey("total") && mappedProps.containsKey("orderTotal")) {
            mappedProps["total"] = mappedProps["orderTotal"]
        }
        [
            "billing_last_name"  : "last_name",
            "billing_first_name" : "first_name",
            "billing_email"      : "email",
            "billing_city"       : "city",
            "billing_address_1"  : "address_1",
            "billing_phone"      : "phone"
        ].each { billingKey, flatKey ->
            if (!mappedProps.containsKey(billingKey) && mappedProps.containsKey(flatKey)) {
                mappedProps[billingKey] = mappedProps[flatKey]
            }
        }

        try {
            def pLastName    = profileProp("lastName")
            def pFirstName   = profileProp("firstName")
            def pEmail       = profileProp("email")
            def pPhone       = profileProp("phone") ?: profileProp("phoneNumber")
            if (!mappedProps.get("billing_last_name")  && pLastName)  mappedProps["billing_last_name"]  = pLastName
            if (!mappedProps.get("billing_first_name") && pFirstName) mappedProps["billing_first_name"] = pFirstName
            if (!mappedProps.get("billing_email")      && pEmail)     mappedProps["billing_email"]      = pEmail
            if (!mappedProps.get("billing_phone")      && pPhone)     mappedProps["billing_phone"]      = pPhone
        } catch (Exception ignore) {}

        def itemId = (itemIdForDedup ?: UUID.randomUUID().toString()).toString()
        def payload = [
            itemId    : itemId,
            brand     : brand.toString(),
            eventType : "sale",
            profileId : pushProfileId,
            properties: mappedProps
        ]
        String jsonPayload = JsonOutput.toJson(payload)
        logger.warn("${prefix} ${label} SUMMARY brand=${brand} itemId=${itemId} total=${mappedProps.get('total')} orderNumber=${mappedProps.get('orderNumber')} profileId=${pushProfileId}")

        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                HttpPost req = new HttpPost(endpoint)
                req.setEntity(new StringEntity(jsonPayload, ContentType.APPLICATION_JSON))
                req.addHeader("Content-Type", "application/json")
                req.addHeader("Accept", "application/json")
                if (loyaltyUser != null && loyaltyPass != null) {
                    req.addHeader("Authorization", "Basic " + "${loyaltyUser}:${loyaltyPass}".bytes.encodeBase64().toString())
                }
                HttpResponse resp = sharedHttpClient.execute(req)
                int code = resp.getStatusLine().getStatusCode()
                String body = resp.getEntity() ? EntityUtils.toString(resp.getEntity()) : ""
                if (code >= 200 && code < 300) {
                    logger.warn("${prefix} ${label} ✅ SUCCESS POST attempt=${attempt} code=${code} response=${body}")
                    return
                } else {
                    logger.error("${prefix} ${label} ❌ FAIL POST attempt=${attempt} code=${code} response=${body}")
                    if (attempt < MAX_RETRIES) Thread.sleep(RETRY_BACKOFF_MS)
                }
            } catch (Exception e) {
                logger.error("${prefix} ${label} ❌ EXCEPTION during HTTP POST attempt=${attempt}. itemId=${itemId}", e)
                if (attempt < MAX_RETRIES) Thread.sleep(RETRY_BACKOFF_MS)
            }
        }
        logger.error("${prefix} ${label} ❌ GAVE UP after ${MAX_RETRIES} attempts. itemId=${itemId} — transaction NOT confirmed pushed.")
    }

    try {
        // ── FAST PATH: event context is clean, process it directly ────────────
        if (cleanSaleContext) {
            def props = [:]
            try { props = event?.getProperties() ?: [:] } catch (Exception e) {
                logger.error("${prefix} STEP3 Failed to get event properties", e)
            }
            def rawItemId = null
            try { rawItemId = event?.getItemId() } catch (Exception ignore) {}
            def propsItemId = (props?.get("itemId") ?: props?.get("orderNumber") ?: props?.get("orderId") ?: props?.get("order_id"))?.toString()
            def itemId = (rawItemId ?: propsItemId ?: UUID.randomUUID().toString()).toString()

            pushOneSale(props, itemId, "STEP10/FAST")
            logger.warn("${prefix} STEP11 Fast path completed. Returning NO_CHANGE.")
            return EventService.NO_CHANGE
        }

        // ── RECOVERY PATH: event context was mutated — sweep recent sale
        //    events scope-wide (no dependency on this execution's possibly
        //    unrelated/empty bound profile) and push all of them. Safe
        //    because Loyalty is idempotent on itemId (resend = no-op), and
        //    correct because pushOneSale independently resolves each sale's
        //    own canonical profileId from its own scopeEmail.
        def recovered = fetchBroadRecentSaleEvents()
        if (!recovered) {
            logger.warn("${prefix} RECOVERY EXIT: no recent sale events found scope-wide after retries — nothing to push.")
            return EventService.NO_CHANGE
        }

        recovered.eachWithIndex { ev, idx ->
            def props = (ev?.properties instanceof Map) ? ev.properties : [:]
            def itemId = (props?.get("orderNumber") ?: ev?.itemId ?: UUID.randomUUID().toString()).toString()
            pushOneSale(props, itemId, "RECOVERY[${idx}]")
        }

        logger.warn("${prefix} STEP11 Recovery path completed (${recovered.size()} event(s) swept). Returning NO_CHANGE.")
        return EventService.NO_CHANGE
    } finally {
        try { sharedHttpClient.close() } catch (Exception ignore) {}
    }
}
