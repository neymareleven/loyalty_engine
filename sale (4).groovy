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

final Logger logger = LoggerFactory.getLogger("sale_to_loyalty_engine")

@Action(id = "sale", actionExecutor = "groovy:sale", parameters = [
    @Parameter(id = "loyaltyUrl", type = "string", multivalued = false),
    @Parameter(id = "loyaltyUsername", type = "string", multivalued = false),
    @Parameter(id = "loyaltyPassword", type = "string", multivalued = false)
])
def execute() {
    final String prefix = "[sale-to-loyalty-debug]"

    // ── STEP 1: Accept 'sale' directly OR 'profileUpdated' as fallback ────────
    //
    //  WHY profileUpdated is accepted:
    //  For NEW profiles, sale-events runs 25+ setPropertyAction items which forces
    //  a large profile write (~1.5s). Unomi processes the resulting profileUpdated
    //  event SYNCHRONOUSLY in the same thread, mutating the `event` variable in
    //  this Groovy context BEFORE this script gets to execute.
    //  By the time this script runs, `event` is profileUpdated — NOT the original sale.
    //  In that case, sale-events has ALREADY written all sale properties to the profile,
    //  so we read them from there (see STEP 3 fallback path).
    //
    //  For EXISTING profiles (e.g. Postman tests), fewer properties change so the
    //  profile write is quick (~364ms) and the event context is NOT mutated — we
    //  receive `sale` directly (fast path).
    def eventType = null
    try { eventType = event?.getEventType() } catch (Exception e) {
        logger.error("${prefix} STEP1 Failed to get eventType", e)
    }

    def targetItemType = null
    def targetItemId = null
    try { targetItemType = event?.getTarget()?.getItemType() } catch (Exception ignore) {}
    try { targetItemId   = event?.getTarget()?.getItemId()   } catch (Exception ignore) {}

    logger.warn("${prefix} STEP1 eventType=${eventType} targetItemType=${targetItemType} targetItemId=${targetItemId}")

    if (eventType == "sale") {
        // Fast path: event context is clean, validate the sale target.
        // NOTE: real-world "sale" events sent by the WordPress plugin use
        // target.itemType = "order" (see production payload), not "saleform".
        // We accept both to stay compatible with any future source that might
        // still use "saleform".
        if (targetItemType != "order" && targetItemType != "saleform") {
            logger.warn("${prefix} STEP1 EXIT: targetItemType is '${targetItemType}' not 'order'/'saleform' - skipping cascade event.")
            return EventService.NO_CHANGE
        }
        logger.warn("${prefix} STEP1 PASSED (sale): confirmed real sale event. targetItemId=${targetItemId}")

    } else {
        // FALLBACK: Event context was mutated during batch event processing.
        // WooCommerce sends sale + view (and others) in the SAME eventcollector request.
        // Unomi processes sequentially in one thread — `event` may now be view,
        // profileUpdated, or any other event by the time this Groovy runs.
        // The rule STILL fired correctly for the original sale event.
        // sale-events already wrote all sale props to the profile, so we read from there.
        logger.warn("${prefix} STEP1 FALLBACK (${eventType}): event context mutated by batch processing. Reading sale data from profile in STEP 3.")
    }

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

    // ── STEP 3: Event properties — or profile properties if context was mutated ─
    //
    //  Fast path  (eventType == "sale"):      read from event.properties
    //  Fallback   (eventType == "profileUpdated"): read known sale fields from
    //             profile.properties (already written by sale-events rule)
    def props = [:]
    if (eventType == "sale") {
        try { props = event?.getProperties() ?: [:] } catch (Exception e) {
            logger.error("${prefix} STEP3 Failed to get event properties", e)
        }
        logger.warn("${prefix} STEP3 source=event props keys=${props?.keySet()}")

    } else {
        // Fallback: sale-events already copied all sale event properties to the profile.
        try {
            def profileProps = profile?.getProperties() ?: [:]
            ["brand","orderNumber","orderDate","orderTotal","total","paymentMethod",
             "productNames","productPrices","productQuantities","productSubtotals",
             "couponCode","woocommerceCartNonce","billing_last_name","billing_first_name",
             "billing_state","billing_address_1","billing_email","email",
             "billing_country","billing_city","billing_company","shipping_city",
             "billing_phone","tva","expedition","remise","scopeEmail","isLoggedIn"].each { k ->
                if (profileProps.containsKey(k)) props[k] = profileProps[k]
            }
        } catch (Exception e) {
            logger.error("${prefix} STEP3 Failed to read sale props from profile", e)
        }
        logger.warn("${prefix} STEP3 source=profile (event context was mutated) props keys=${props?.keySet()}")

        // Safety guard: only continue if a real sale was stored by sale-events
        if (!props?.get("orderNumber")) {
            logger.warn("${prefix} STEP3 EXIT: no orderNumber in profile — this profileUpdated is not sale-related. Skipping.")
            return EventService.NO_CHANGE
        }
    }

    // ── STEP 3.5: isLoggedIn guard ─────────────────────────────────────────────
    //
    //  Only push sales made by a logged-in customer to the Loyalty Engine.
    //  This is a defense-in-depth check: the rule condition already filters on
    //  properties.isLoggedIn == true, but we re-check here because:
    //   (a) the fallback path above reads from profile.properties, which may have
    //       been overwritten by a LATER, different event before this Groovy runs;
    //   (b) it protects the script even if the rule condition is ever modified
    //       or bypassed.
    def isLoggedInRaw = props?.get("isLoggedIn")
    boolean isLoggedIn = (isLoggedInRaw == true) || (isLoggedInRaw?.toString()?.trim()?.equalsIgnoreCase("true"))

    logger.warn("${prefix} STEP3.5 isLoggedInRaw=${isLoggedInRaw} (${isLoggedInRaw?.getClass()?.simpleName}) resolved=${isLoggedIn}")

    if (!isLoggedIn) {
        logger.warn("${prefix} STEP3.5 EXIT: isLoggedIn is not true (value=${isLoggedInRaw}) - guest/anonymous sale, skipping loyalty push. profileId=${profileId}")
        return EventService.NO_CHANGE
    }

    // ── STEP 4: Brand resolution ──────────────────────────────────────────────
    def brand = (props?.get("brand") ?: profile?.getProperty("brand"))?.toString()
    logger.warn("${prefix} STEP4 brand from props/profile=${brand}")

    if (!brand || !brand.trim()) {
        try {
            def scope = event?.getScope()?.toString()
            logger.warn("${prefix} STEP4 brand not in props/profile, trying scope=${scope}")
            brand = scope
        } catch (Exception e) {
            logger.error("${prefix} STEP4 Failed to get scope", e)
        }
    }

    logger.warn("${prefix} STEP4 final brand=${brand}")

    if (!brand || !brand.trim()) {
        logger.error("${prefix} STEP4 EXIT: Missing brand - skipping. profileId=${profileId}")
        return EventService.NO_CHANGE
    }

    // ── STEP 5: Action parameters ─────────────────────────────────────────────
    //
    //  ROOT CAUSE FIX: Unomi's GroovyActionDispatcher can leak stale `action` bindings
    //  across Groovy scripts running in the same thread/request. When another Groovy
    //  action (e.g. loyalty_profile, batira) ran just before this one, action.getParameterValues()
    //  returns THEIR parameters (mauticUrl = marketing.qilinsa.com) instead of ours.
    //
    //  FIX: Always fetch parameters fresh from /cxs/rules/sale-to-loyalty via REST API.
    //  This is the only reliable source of truth for the loyalty URL and credentials.
    def loyaltyUrl  = null
    def loyaltyUser = null
    def loyaltyPass = null

    // Primary: fetch fresh from rule API (bypasses stale action binding)
    try {
        def trustAll = [
            getAcceptedIssuers   : { [] as java.security.cert.X509Certificate[] },
            checkClientTrusted   : { chain, authType -> },
            checkServerTrusted   : { chain, authType -> }
        ] as javax.net.ssl.X509TrustManager
        def sslCtx = javax.net.ssl.SSLContext.getInstance("TLS")
        sslCtx.init(null, [trustAll] as javax.net.ssl.TrustManager[], new java.security.SecureRandom())

        def ruleApiUrl = new URL("https://localhost:9443/cxs/rules/sale-to-loyalty")
        def conn = (javax.net.ssl.HttpsURLConnection) ruleApiUrl.openConnection()
        conn.setSSLSocketFactory(sslCtx.socketFactory)
        conn.setHostnameVerifier { h, s -> true }
        conn.setRequestProperty("Authorization", "Basic " + "karaf:karaf".bytes.encodeBase64().toString())
        conn.setConnectTimeout(3000)
        conn.setReadTimeout(3000)

        def ruleJson = new groovy.json.JsonSlurper().parse(conn.inputStream)
        def saleAction = ruleJson?.actions?.find { it.type == "sale" }
        def freshParams = saleAction?.parameterValues ?: [:]
        loyaltyUrl  = freshParams.get("loyaltyUrl")?.toString()?.trim()
        loyaltyUser = freshParams.get("loyaltyUsername")?.toString()
        loyaltyPass = freshParams.get("loyaltyPassword")?.toString()
        logger.warn("${prefix} STEP5 fetched fresh from rule API: loyaltyUrl=${loyaltyUrl} user=${loyaltyUser}")
    } catch (Exception e) {
        logger.warn("${prefix} STEP5 rule API fetch failed (${e.message}), falling back to action binding")
        // Fallback: try action binding (may be stale if Groovy context was re-used)
        try {
            def allParams = action?.getParameterValues()
            logger.warn("${prefix} STEP5 action binding keys=${allParams?.keySet()}")
            loyaltyUrl  = (allParams?.get("loyaltyUrl")  ?: allParams?.get("mauticUrl"))?.toString()?.trim()
            loyaltyUser = (allParams?.get("loyaltyUsername") ?: allParams?.get("mauticUsername"))?.toString()
            loyaltyPass = (allParams?.get("loyaltyPassword") ?: allParams?.get("mauticPassword"))?.toString()
        } catch (Exception e2) {
            logger.error("${prefix} STEP5 action binding also failed", e2)
        }
    }

    logger.warn("${prefix} STEP5 loyaltyUrl=${loyaltyUrl} loyaltyUser=${loyaltyUser} loyaltyPassSet=${loyaltyPass != null}")

    if (!loyaltyUrl) {
        logger.error("${prefix} STEP5 EXIT: Missing loyaltyUrl - cannot push sale event.")
        return EventService.NO_CHANGE
    }

    def endpoint = loyaltyUrl.endsWith("/") ? (loyaltyUrl + "transactions") : (loyaltyUrl + "/transactions")
    logger.warn("${prefix} STEP5 endpoint=${endpoint}")

    // ── STEP 6: Item ID (de-duplication key) ──────────────────────────────────
    def rawItemId = null
    try { rawItemId = event?.getItemId() } catch (Exception ignore) {}

    def propsItemId = (
        props?.get("itemId")      ?:
        props?.get("orderNumber") ?:
        props?.get("orderId")     ?:
        props?.get("order_id")
    )?.toString()

    // When event context is mutated, rawItemId belongs to the wrong event (e.g. the view event).
    // Use the stable orderNumber-based propsItemId for deduplication on the loyalty side.
    def itemId = (eventType == "sale" ? rawItemId : null) ?: propsItemId ?: UUID.randomUUID().toString()
    itemId = itemId.toString()
    logger.warn("${prefix} STEP6 itemId=${itemId} rawItemId=${rawItemId} propsItemId=${propsItemId} (used stable=${eventType != 'sale'})")

    // ── STEP 7: Build mappedProps with field aliasing ─────────────────────────
    def mappedProps = [:]
    if (props instanceof Map) { mappedProps.putAll(props) }

    // orderTotal -> total
    if (!mappedProps.containsKey("total") && mappedProps.containsKey("orderTotal")) {
        mappedProps["total"] = mappedProps["orderTotal"]
        logger.warn("${prefix} STEP7 aliased orderTotal -> total = ${mappedProps['total']}")
    }

    // flat fields -> billing_* fields
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
            logger.warn("${prefix} STEP7 aliased ${flatKey} -> ${billingKey}")
        }
    }

    // ── STEP 8: Enrich missing billing fields from Unomi profile ──────────────
    def pLastName    = null
    def pFirstName   = null
    def pEmail       = null
    def pPhoneNumber = null
    def pPhone       = null
    try { pLastName    = profile?.getProperty("lastName")    } catch (Exception ignore) {}
    try { pFirstName   = profile?.getProperty("firstName")   } catch (Exception ignore) {}
    try { pEmail       = profile?.getProperty("email")       } catch (Exception ignore) {}
    try { pPhoneNumber = profile?.getProperty("phoneNumber") } catch (Exception ignore) {}
    try { pPhone       = profile?.getProperty("phone")       } catch (Exception ignore) {}

    logger.warn("${prefix} STEP8 profile props: lastName=${pLastName} firstName=${pFirstName} email=${pEmail} phone=${pPhone ?: pPhoneNumber}")

    if (!mappedProps.get("billing_last_name")  && pLastName)                { mappedProps["billing_last_name"]  = pLastName  }
    if (!mappedProps.get("billing_first_name") && pFirstName)               { mappedProps["billing_first_name"] = pFirstName }
    if (!mappedProps.get("billing_email")      && pEmail)                   { mappedProps["billing_email"]      = pEmail     }
    if (!mappedProps.get("email")              && pEmail)                   { mappedProps["email"]              = pEmail     }
    if (!mappedProps.get("billing_phone")      && (pPhoneNumber ?: pPhone)) { mappedProps["billing_phone"]      = pPhoneNumber ?: pPhone }

    // ── STEP 9: Build final payload ───────────────────────────────────────────
    def payload = [
        itemId    : itemId,
        brand     : brand.toString(),
        eventType : "sale",
        profileId : profileId.toString(),
        properties: mappedProps
    ]

    String jsonPayload = JsonOutput.toJson(payload)

    logger.warn("${prefix} STEP9 FULL PAYLOAD=${jsonPayload}")
    logger.warn("${prefix} STEP9 SUMMARY brand=${brand} profileId=${profileId} itemId=${itemId} total=${mappedProps.get('total')} billing_email=${mappedProps.get('billing_email') ?: mappedProps.get('email')}")

    // ── STEP 10: HTTP POST to Loyalty Engine ──────────────────────────────────
    logger.warn("${prefix} STEP10 Attempting POST to ${endpoint}")

    RequestConfig requestConfig = RequestConfig.custom()
        .setConnectTimeout(15000)
        .setConnectionRequestTimeout(15000)
        .setSocketTimeout(120000)
        .build()
    CloseableHttpClient httpClient = HttpClientBuilder.create()
        .setDefaultRequestConfig(requestConfig)
        .build()
    try {
        HttpPost req = new HttpPost(endpoint)
        req.setEntity(new StringEntity(jsonPayload, ContentType.APPLICATION_JSON))
        req.addHeader("Content-Type", "application/json")
        req.addHeader("Accept", "application/json")

        if (loyaltyUser != null && loyaltyPass != null) {
            def authHeader = "${loyaltyUser}:${loyaltyPass}".bytes.encodeBase64().toString()
            req.addHeader("Authorization", "Basic " + authHeader)
            logger.warn("${prefix} STEP10 Basic auth added for user=${loyaltyUser}")
        } else {
            logger.warn("${prefix} STEP10 No auth credentials - sending without Authorization header")
        }

        HttpResponse resp = httpClient.execute(req)
        int code = resp.getStatusLine().getStatusCode()
        String body = resp.getEntity() ? EntityUtils.toString(resp.getEntity()) : ""

        if (code >= 200 && code < 300) {
            logger.warn("${prefix} STEP10 ✅ SUCCESS POST ${endpoint} code=${code} response=${body}")
        } else {
            logger.error("${prefix} STEP10 ❌ FAIL POST ${endpoint} code=${code} response=${body}")
        }

    } catch (Exception e) {
        logger.error("${prefix} STEP10 ❌ EXCEPTION during HTTP POST. endpoint=${endpoint} brand=${brand} profileId=${profileId}", e)
    } finally {
        try { httpClient.close() } catch (Exception ignore) {}
    }

    // ── STEP 11: Done ─────────────────────────────────────────────────────────
    logger.warn("${prefix} STEP11 Script completed successfully. Returning NO_CHANGE.")
    return EventService.NO_CHANGE
}
