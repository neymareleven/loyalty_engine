import org.apache.unomi.api.Profile
import org.apache.unomi.api.services.EventService
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.util.EntityUtils
import org.apache.http.HttpResponse
import org.apache.http.entity.ContentType
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import groovy.json.JsonOutput

final Logger logger = LoggerFactory.getLogger("loyalty_profile")

@Action(id = "loyalty_profile", actionExecutor = "groovy:loyalty_profile", parameters = [
    @Parameter(id = "loyaltyUrl", type = "string", multivalued = false),
    @Parameter(id = "loyaltyUsername", type = "string", multivalued = false),
    @Parameter(id = "loyaltyPassword", type = "string", multivalued = false)
])
def execute() {

    // ── 0. Log event type for debugging ──────────────────────────────────────
    def eventType = null
    try { eventType = event.getEventType() } catch (Exception ignore) {}
    logger.info("[loyalty_profile] Triggered by eventType=${eventType}")

    // ── 1. Resolve profile ────────────────────────────────────────────────────
    Profile profile = null
    try { profile = event.getProfile() } catch (Exception ignore) {}

    def profileId = null
    try {
        profileId = event.getProfileId()
    } catch (Exception ignore) {
        profileId = profile?.getItemId()
    }

    if (!profileId || !profileId.toString().trim()) {
        logger.warn("[loyalty_profile] Missing profileId; skipping profile upsert.")
        return EventService.NO_CHANGE
    }
    profileId = profileId.toString().trim()

    // ── 2. Collect event properties ──────────────────────────────────────────
    def eventProps = [:]
    try { eventProps = event.getProperties() ?: [:] } catch (Exception ignore) {}

    // ── 2b. Flatten CF7 / Unomi form field values ────────────────────────────
    // Form events (Contact Form 7) store answers in nested flattenedProperties.fields
    // and/or at the top level (email, firstName, your-brand, …).
    def formFields = [:]
    try {
        def fp = eventProps?.get("flattenedProperties")
        if (fp instanceof Map) {
            def fields = fp.get("fields")
            if (fields instanceof Map) {
                formFields.putAll(fields)
            }
        }
    } catch (Exception ignore) {}
    eventProps.each { k, v ->
        def key = k?.toString()
        if (!key || key.startsWith("_") || v == null || v instanceof Map || v instanceof List) {
            return
        }
        if (!formFields.containsKey(key)) {
            formFields[key] = v
        }
    }
    if (formFields) {
        logger.debug("[loyalty_profile] flattenedProperties.fields keys: ${formFields.keySet()}")
    }

    // Skip echo when Loyalty Engine just pushed loyalty fields (POST /cxs/profiles).
    if (eventType == "profileUpdated") {
        def loyaltyLinked = null
        try { loyaltyLinked = profile?.getProperty("loyaltyEngineCustomerId") } catch (Exception ignore) {}
        if (loyaltyLinked) {
            def loyaltyOnlyKeys = [
                "loyaltyStatus", "statusPoints", "loyaltyCustomerStatus", "loyaltyTierName",
                "loyaltyEngineCustomerId", "loyaltyEngineSyncedAt", "loyaltyPointsBalance",
                "lastActivityAt", "loyaltyStatusAssignedAt", "loyaltyStatusExpiresAt",
                "pointsExpiresAt", "statusPointsResetAt", "loyaltyCreatedAt", "loyaltyUpdatedAt",
                "unomiProfileId", "unomi_profile_id", "metrics", "firstVisit", "lastVisit"
            ] as Set
            def hasNonLoyaltyDelta = eventProps.keySet().any { k ->
                !loyaltyOnlyKeys.contains(k?.toString())
            }
            if (!hasNonLoyaltyDelta) {
                logger.debug("[loyalty_profile] Skipping profileUpdated (loyalty sync echo)")
                return EventService.NO_CHANGE
            }
        }
    }

    // ── 3. Resolve brand — exhaustive fallback chain ─────────────────────────
    //
    //  WHY this is needed:
    //  When the rule uses profileUpdatedEventCondition, the `event` object in
    //  the action is the internal Unomi "profileUpdated" event — NOT the original
    //  contactInfoSubmitted event. That internal event has NO brand in its
    //  properties. So we must fall back to event.scope (always set on the
    //  original event envelope) and then the profile property.
    //
    //  Priority order:
    //    a) event.properties.brand   ← present when rule fires on contactInfoSubmitted directly
    //    b) event.scope              ← always set; e.g. "batira" from the event envelope
    //    c) profile.brand            ← already-enriched profiles

    def brand = null

    // (a) event.properties.brand or CF7 your-brand
    def brandFromProps = eventProps?.get("brand")?.toString()?.trim()
    if (brandFromProps) {
        brand = brandFromProps
        logger.debug("[loyalty_profile] brand resolved from event.properties: ${brand}")
    }
    if (!brand) {
        def brandFromForm = formFields?.get("your-brand")?.toString()?.trim()
        if (brandFromForm) {
            brand = brandFromForm
            logger.debug("[loyalty_profile] brand resolved from event.properties/formFields: ${brand}")
        }
    }

    // (b) event.scope — set in the event JSON envelope; not null-safe to skip
    if (!brand) {
        try {
            def scope = event.getScope()?.toString()?.trim()
            if (scope && scope != "systemscope") {
                brand = scope
                logger.debug("[loyalty_profile] brand resolved from event.scope: ${brand}")
            }
        } catch (Exception ignore) {}
    }

    // (c) profile.getProperty("brand") — for already-enriched profiles
    if (!brand) {
        try {
            def profileBrand = profile?.getProperty("brand")?.toString()?.trim()
            if (profileBrand) {
                brand = profileBrand
                logger.debug("[loyalty_profile] brand resolved from profile.getProperty: ${brand}")
            }
        } catch (Exception ignore) {}
    }

    if (!brand) {
        logger.warn("[loyalty_profile] Missing brand for profileId=${profileId} eventType=${eventType}; skipping profile upsert.")
        return EventService.NO_CHANGE
    }

    // ── 4. Read action parameters ─────────────────────────────────────────────
    def loyaltyUrl  = action.getParameterValues().get("loyaltyUrl")?.toString()?.trim()
    def loyaltyUser = action.getParameterValues().get("loyaltyUsername")?.toString()
    def loyaltyPass = action.getParameterValues().get("loyaltyPassword")?.toString()

    if (!loyaltyUrl) {
        logger.error("[loyalty_profile] Missing loyaltyUrl parameter; cannot upsert profile.")
        return EventService.NO_CHANGE
    }

    // ── 5. Resolve optional profile fields ───────────────────────────────────
    // gender
    def gender = (eventProps?.get("gender") ?: profile?.getProperty("gender"))?.toString()?.trim() ?: null

    // birthdate — accepts YYYY-MM-DD or MM-DD
    def birthdate = null
    def bdRaw = eventProps?.get("birthdate") ?: eventProps?.get("birthDate")
    if (bdRaw != null) {
        birthdate = bdRaw.toString().trim() ?: null
    }
    if (!birthdate) {
        def profileBd = null
        try { profileBd = profile?.getProperty("birthDate") ?: profile?.getProperty("birthdate") } catch (Exception ignore) {}
        if (profileBd != null) {
            birthdate = profileBd.toString().trim() ?: null
        }
    }

    // ── 6. Build properties sub-object ───────────────────────────────────────
    // Forward form/contact fields; drop CF7 internals (_wpcf7*, phone-cf7it*).
    def properties = [:]
    formFields.each { k, v ->
        def key = k?.toString()
        if (!key || key.startsWith("_") || key.startsWith("phone-cf7it")) {
            return
        }
        if (key == "your-brand" || key == "your_brand") {
            return
        }
        properties[key] = v
    }
    if (eventProps) {
        eventProps.each { k, v ->
            def key = k?.toString()
            if (!key || key.startsWith("_") || key == "flattenedProperties" || v == null) {
                return
            }
            if (!properties.containsKey(key)) {
                properties[key] = v
            }
        }
    }
    ["firstName", "lastName", "email", "phoneNumber", "phone", "scopeEmail"].each { field ->
        if (!properties.containsKey(field)) {
            try {
                def val = profile?.getProperty(field)
                if (val != null) properties[field] = val
            } catch (Exception ignore) {}
        }
    }

    // ── 7. Build the CustomerUpsert payload ──────────────────────────────────
    def payload = [
        brand     : brand,
        profileId : profileId,
        properties: properties
    ]
    if (gender)    payload["gender"]    = gender
    if (birthdate) payload["birthdate"] = birthdate

    String jsonPayload = JsonOutput.toJson(payload)
    logger.info("[loyalty_profile] Sending upsert: brand=${brand} profileId=${profileId} gender=${gender} birthdate=${birthdate}")

    // ── 8. POST to /customers/upsert ─────────────────────────────────────────
    def endpoint = loyaltyUrl.endsWith("/")
        ? (loyaltyUrl + "customers/upsert")
        : (loyaltyUrl + "/customers/upsert")

    CloseableHttpClient httpClient = HttpClientBuilder.create().build()
    try {
        HttpPost req = new HttpPost(endpoint)
        req.setEntity(new StringEntity(jsonPayload, ContentType.APPLICATION_JSON))
        req.addHeader("Content-Type", "application/json")
        req.addHeader("Accept",       "application/json")
        req.addHeader("X-Profile-Sync-Source", "unomi")

        if (loyaltyUser != null && loyaltyPass != null) {
            def authHeader = "${loyaltyUser}:${loyaltyPass}".bytes.encodeBase64().toString()
            req.addHeader("Authorization", "Basic " + authHeader)
        }

        HttpResponse resp = httpClient.execute(req)
        int code = resp.getStatusLine().getStatusCode()
        String body = resp.getEntity() ? EntityUtils.toString(resp.getEntity()) : ""

        if (code >= 200 && code < 300) {
            logger.info("[loyalty_profile] Profile upserted. brand=${brand} profileId=${profileId} code=${code}")
        } else {
            logger.error("[loyalty_profile] Profile upsert failed. brand=${brand} profileId=${profileId} code=${code} body=${body?.substring(0, Math.min(800, body.length()))}")
        }
    } catch (Exception e) {
        logger.error("[loyalty_profile] Error upserting profile. brand=${brand} profileId=${profileId}", e)
    } finally {
        try { httpClient.close() } catch (Exception ignore) {}
    }

    return EventService.NO_CHANGE
}
