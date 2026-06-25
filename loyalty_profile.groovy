import org.apache.unomi.api.Profile
import org.apache.unomi.api.services.EventService
import org.apache.unomi.api.services.ProfileService
import org.apache.unomi.groovy.actions.GroovyActionDispatcher
import org.osgi.framework.FrameworkUtil
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

    def eventType = null
    try { eventType = event.getEventType() } catch (Exception ignore) {}
    logger.info("[loyalty_profile] Triggered by eventType=${eventType}")

    Profile profile = null
    try { profile = event.getProfile() } catch (Exception ignore) {}

    def sessionProfileId = null
    try {
        sessionProfileId = event.getProfileId()
    } catch (Exception ignore) {
        sessionProfileId = profile?.getItemId()
    }
    if (!sessionProfileId || !sessionProfileId.toString().trim()) {
        logger.warn("[loyalty_profile] Missing profileId; skipping profile upsert.")
        return EventService.NO_CHANGE
    }
    sessionProfileId = sessionProfileId.toString().trim()
    def profileId = sessionProfileId

    def eventProps = [:]
    try { eventProps = event.getProperties() ?: [:] } catch (Exception ignore) {}

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

    ProfileService profileService = null
    try {
        def bundle = FrameworkUtil.getBundle(GroovyActionDispatcher.class)
        def ref = bundle?.getBundleContext()?.getServiceReference(ProfileService.class)
        if (ref) {
            profileService = bundle.getBundleContext().getService(ref)
        }
    } catch (Exception e) {
        logger.debug("[loyalty_profile] ProfileService unavailable: ${e.message}")
    }

    def resolveScopeEmail = { String brandValue, String emailValue, String scopeFromForm ->
        def se = scopeFromForm?.toString()?.trim()
        if (se) {
            return se.toLowerCase()
        }
        def em = emailValue?.toString()?.trim()?.toLowerCase()
        if (brandValue && em && em.contains("@")) {
            return "${brandValue}-${em}".toLowerCase()
        }
        return null
    }

    def pickBestProfileForEmail = { Collection profiles, String emailValue ->
        if (!profiles || !emailValue) {
            return null
        }
        def target = emailValue.trim().toLowerCase()
        Profile best = null
        profiles.each { Profile candidate ->
            if (!candidate) {
                return
            }
            def candidateEmail = candidate.getProperty("email")?.toString()?.trim()?.toLowerCase()
            if (candidateEmail != target) {
                return
            }
            if (best == null) {
                best = candidate
                return
            }
            def lu = candidate.getSystemProperties()?.get("lastUpdated")?.toString()
            def blu = best.getSystemProperties()?.get("lastUpdated")?.toString()
            if (lu && blu && lu > blu) {
                best = candidate
            }
        }
        return best
    }

    def findProfilesByProperty = { ProfileService ps, String propertyName, String propertyValue ->
        if (!ps || !propertyName || !propertyValue) {
            return []
        }
        try {
            def result = ps.findProfilesByPropertyValue(propertyName, propertyValue, 0, 10, "systemProperties.lastUpdated:desc")
            if (result?.list) {
                return result.list
            }
        } catch (Exception e) {
            logger.debug("[loyalty_profile] findProfilesByPropertyValue(${propertyName}) failed: ${e.message}")
        }
        return []
    }

    def resolveCanonicalProfileId = { ProfileService ps, String brandValue, String emailValue, String scopeFromForm, String fallbackProfileId ->
        if (!ps || !emailValue?.trim()) {
            return fallbackProfileId
        }
        def emailNorm = emailValue.trim().toLowerCase()
        def scopeEmail = resolveScopeEmail(brandValue, emailNorm, scopeFromForm)
        def candidates = [] as LinkedHashSet
        if (scopeEmail) {
            candidates.addAll(findProfilesByProperty(ps, "properties.scopeEmail", scopeEmail))
        }
        candidates.addAll(findProfilesByProperty(ps, "properties.email", emailNorm))
        def best = pickBestProfileForEmail(candidates, emailNorm)
        if (best?.itemId) {
            return best.itemId.toString().trim()
        }
        return fallbackProfileId
    }

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
        } else {
            def regEmail = profile?.getProperty("email")?.toString()?.trim()
            def regBrand = profile?.getProperty("brand")?.toString()?.trim()
            def regFirstName = profile?.getProperty("firstName")?.toString()?.trim()
            if (!regEmail || !regBrand || !regFirstName) {
                logger.debug("[loyalty_profile] Skipping profileUpdated (not registration-like)")
                return EventService.NO_CHANGE
            }
            logger.info("[loyalty_profile] profileUpdated registration backfill profileId=${profileId} email=${regEmail}")
        }
    }

    def brand = null
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
    if (!brand) {
        try {
            def scope = event.getScope()?.toString()?.trim()
            if (scope && scope != "systemscope") {
                brand = scope
                logger.debug("[loyalty_profile] brand resolved from event.scope: ${brand}")
            }
        } catch (Exception ignore) {}
    }
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

    def formEmail = (formFields?.get("email") ?: eventProps?.get("email") ?: profile?.getProperty("email"))?.toString()?.trim()
    def scopeEmailForm = formFields?.get("scopeEmail")?.toString()?.trim()
    def sessionEmail = profile?.getProperty("email")?.toString()?.trim()

    if profileService && formEmail) {
        def formEmailNorm = formEmail.toLowerCase()
        def resolvedId = resolveCanonicalProfileId(
            profileService,
            brand,
            formEmailNorm,
            scopeEmailForm,
            sessionProfileId
        )
        if (resolvedId && resolvedId != sessionProfileId) {
            logger.info(
                "[loyalty_profile] Canonical profile resolved: session=${sessionProfileId} " +
                "sessionEmail=${sessionEmail} formEmail=${formEmailNorm} -> ${resolvedId}"
            )
            profileId = resolvedId
            try {
                profile = profileService.load(profileId)
            } catch (Exception ignore) {}
        } else if (eventType == "form") {
            logger.info(
                "[loyalty_profile] Using session profileId=${sessionProfileId} for formEmail=${formEmailNorm} " +
                "(canonical search returned same id; profileUpdated backfill may follow)"
            )
        }
    }

    def loyaltyUrl  = action.getParameterValues().get("loyaltyUrl")?.toString()?.trim()
    def loyaltyUser = action.getParameterValues().get("loyaltyUsername")?.toString()
    def loyaltyPass = action.getParameterValues().get("loyaltyPassword")?.toString()

    if (!loyaltyUrl) {
        logger.error("[loyalty_profile] Missing loyaltyUrl parameter; cannot upsert profile.")
        return EventService.NO_CHANGE
    }

    def gender = (eventProps?.get("gender") ?: formFields?.get("gender") ?: profile?.getProperty("gender"))?.toString()?.trim() ?: null

    def birthdate = null
    def bdRaw = eventProps?.get("birthdate") ?: eventProps?.get("birthDate") ?: formFields?.get("birthdate")
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

    def payload = [
        brand     : brand,
        profileId : profileId,
        properties: properties
    ]
    if (gender)    payload["gender"]    = gender
    if (birthdate) payload["birthdate"] = birthdate

    def postUpsert = { Map body ->
        String jsonPayload = JsonOutput.toJson(body)
        logger.info("[loyalty_profile] Sending upsert: brand=${body.brand} profileId=${body.profileId} gender=${body.gender} birthdate=${body.birthdate}")

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
            String respBody = resp.getEntity() ? EntityUtils.toString(resp.getEntity()) : ""

            if (code >= 200 && code < 300) {
                logger.info("[loyalty_profile] Profile upserted. brand=${body.brand} profileId=${body.profileId} code=${code}")
            } else {
                logger.error("[loyalty_profile] Profile upsert failed. brand=${body.brand} profileId=${body.profileId} code=${code} body=${respBody?.substring(0, Math.min(800, respBody.length()))}")
            }
            return code
        } catch (Exception e) {
            logger.error("[loyalty_profile] Error upserting profile. brand=${body.brand} profileId=${body.profileId}", e)
            return 0
        } finally {
            try { httpClient.close() } catch (Exception ignore) {}
        }
    }

    postUpsert(payload)

    // After mergeProfilesOnEmail, the canonical profile id may appear a moment later.
    if (eventType == "form" && profileService && formEmail) {
        try {
            Thread.sleep(2500)
            def lateId = resolveCanonicalProfileId(
                profileService,
                brand,
                formEmail.toLowerCase(),
                scopeEmailForm,
                profileId
            )
            if (lateId && lateId != profileId) {
                logger.info("[loyalty_profile] Post-merge reconcile upsert: ${profileId} -> ${lateId}")
                def retryPayload = new LinkedHashMap(payload)
                retryPayload.profileId = lateId
                try {
                    profile = profileService.load(lateId)
                } catch (Exception ignore) {}
                ["firstName", "lastName", "email", "phoneNumber", "phone", "scopeEmail"].each { field ->
                    try {
                        def val = profile?.getProperty(field)
                        if (val != null) retryPayload.properties[field] = val
                    } catch (Exception ignore) {}
                }
                postUpsert(retryPayload)
            }
        } catch (Exception e) {
            logger.debug("[loyalty_profile] Post-merge reconcile skipped: ${e.message}")
        }
    }

    return EventService.NO_CHANGE
}
