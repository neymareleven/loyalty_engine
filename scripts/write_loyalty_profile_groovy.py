"""Write loyalty_profile.groovy as UTF-8 (fixes UTF-16 corruption on Windows)."""
from pathlib import Path

CONTENT = r'''import org.apache.unomi.api.Profile
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
    def sessionProfileId = profileId

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
        logger.debug("[loyalty_profile] form fields keys: ${formFields.keySet()}")
    }

    def brand = null
    def brandFromProps = eventProps?.get("brand")?.toString()?.trim()
    if (brandFromProps) {
        brand = brandFromProps
        logger.debug("[loyalty_profile] brand from event.properties: ${brand}")
    }
    if (!brand) {
        def brandFromForm = formFields?.get("your-brand")?.toString()?.trim()
        if (brandFromForm) {
            brand = brandFromForm
            logger.debug("[loyalty_profile] brand from formFields: ${brand}")
        }
    }
    if (!brand) {
        try {
            def scope = event.getScope()?.toString()?.trim()
            if (scope && scope != "systemscope") {
                brand = scope
                logger.debug("[loyalty_profile] brand from event.scope: ${brand}")
            }
        } catch (Exception ignore) {}
    }
    if (!brand) {
        try {
            def profileBrand = profile?.getProperty("brand")?.toString()?.trim()
            if (profileBrand) {
                brand = profileBrand
                logger.debug("[loyalty_profile] brand from profile: ${brand}")
            }
        } catch (Exception ignore) {}
    }

    if (!brand) {
        logger.warn("[loyalty_profile] Missing brand for profileId=${profileId} eventType=${eventType}; skipping.")
        return EventService.NO_CHANGE
    }

    ProfileService profileService = null
    try {
        def bundle = FrameworkUtil.getBundle(GroovyActionDispatcher.class)
        def ref = bundle?.getBundleContext()?.getServiceReference(ProfileService.class)
        if (ref) {
            profileService = bundle.getBundleContext().getService(ref)
        }
    } catch (Exception ignore) {}

    def findProfilesByProperty = { ProfileService ps, String propertyName, String propertyValue ->
        if (!ps || !propertyName || !propertyValue) {
            return []
        }
        try {
            def result = ps.findProfilesByPropertyValue(propertyName, propertyValue, 0, 10, "systemProperties.lastUpdated:desc")
            return result?.list ?: []
        } catch (Exception e) {
            logger.debug("[loyalty_profile] findProfilesByPropertyValue(${propertyName}) failed: ${e.message}")
            return []
        }
    }

    def pickNewestProfile = { Collection profiles ->
        Profile best = null
        profiles?.each { Profile candidate ->
            if (!candidate) {
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

    def formEmail = formFields?.get("email")?.toString()?.trim()?.toLowerCase()
    if (profileService && formEmail) {
        def scopeEmail = (formFields?.get("scopeEmail") ?: "${brand}-${formEmail}").toString().trim().toLowerCase()
        def candidates = [] as LinkedHashSet
        candidates.addAll(findProfilesByProperty(profileService, "properties.scopeEmail", scopeEmail))
        candidates.addAll(findProfilesByProperty(profileService, "properties.email", formEmail))
        def best = pickNewestProfile(candidates)
        if (best?.itemId) {
            def resolvedId = best.itemId.toString().trim()
            if (resolvedId != sessionProfileId) {
                logger.info("[loyalty_profile] Canonical profile: session=${sessionProfileId} -> ${resolvedId} email=${formEmail}")
                profileId = resolvedId
                try { profile = profileService.load(profileId) } catch (Exception ignore) {}
            }
        }
    }

    def loyaltyUrl  = action.getParameterValues().get("loyaltyUrl")?.toString()?.trim()
    def loyaltyUser = action.getParameterValues().get("loyaltyUsername")?.toString()
    def loyaltyPass = action.getParameterValues().get("loyaltyPassword")?.toString()

    if (!loyaltyUrl) {
        logger.error("[loyalty_profile] Missing loyaltyUrl parameter; cannot upsert profile.")
        return EventService.NO_CHANGE
    }

    def gender = (formFields?.get("gender") ?: eventProps?.get("gender") ?: profile?.getProperty("gender"))?.toString()?.trim() ?: null

    def birthdate = null
    def bdRaw = formFields?.get("birthdate") ?: formFields?.get("birthDate") ?: eventProps?.get("birthdate") ?: eventProps?.get("birthDate")
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
        if (key in ["your-brand", "your_brand", "password-53", "password-54"]) {
            return
        }
        properties[key] = v
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
    if (formEmail) payload["email"] = formEmail

    String jsonPayload = JsonOutput.toJson(payload)
    logger.info("[loyalty_profile] Sending upsert: brand=${brand} profileId=${profileId} sessionProfileId=${sessionProfileId}")

    def endpoint = loyaltyUrl.endsWith("/")
        ? (loyaltyUrl + "customers/upsert")
        : (loyaltyUrl + "/customers/upsert")

    CloseableHttpClient httpClient = HttpClientBuilder.create().build()
    try {
        HttpPost req = new HttpPost(endpoint)
        req.setEntity(new StringEntity(jsonPayload, ContentType.APPLICATION_JSON))
        req.addHeader("Content-Type", "application/json")
        req.addHeader("Accept", "application/json")
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
'''

path = Path(__file__).resolve().parents[1] / "loyalty_profile.groovy"
path.write_text(CONTENT, encoding="utf-8", newline="\n")
print(f"Wrote {path} ({path.stat().st_size} bytes)")
