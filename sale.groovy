import org.apache.unomi.api.Profile
import org.apache.unomi.api.services.EventService
import org.apache.unomi.api.services.ProfileService
import org.apache.unomi.groovy.actions.GroovyActionDispatcher
import org.osgi.framework.FrameworkUtil
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
    def eventType = null
    try { eventType = event.getEventType() } catch (Exception ignore) {}

    if (!eventType || eventType != "sale") {
        return EventService.NO_CHANGE
    }
    logger.info("[sale] Triggered orderNumber=${event.getProperties()?.get('orderNumber')} profileId=${event.getProfileId()}")

    Profile profile = null
    try { profile = event.getProfile() } catch (Exception ignore) {}

    def sessionProfileId = null
    try {
        sessionProfileId = event.getProfileId()
    } catch (Exception ignore) {
        sessionProfileId = profile?.getItemId()
    }

    if (!sessionProfileId || !sessionProfileId.toString().trim()) {
        logger.warn("[sale] Missing profileId; skipping sale push.")
        return EventService.NO_CHANGE
    }
    sessionProfileId = sessionProfileId.toString().trim()
    def profileId = sessionProfileId

    def props = [:]
    try { props = event.getProperties() ?: [:] } catch (Exception ignore) {}

    def brand = (props?.get("brand") ?: profile?.getProperty("brand"))?.toString()?.trim()
    if (!brand) {
        try {
            def scope = event.getScope()?.toString()?.trim()
            if (scope && scope != "systemscope") {
                brand = scope
                logger.debug("[sale] brand resolved from event.scope: ${brand}")
            }
        } catch (Exception ignore) {}
    }
    if (!brand) {
        logger.warn("[sale] Missing brand; skipping. profileId=${profileId}")
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
            logger.debug("[sale] findProfilesByPropertyValue(${propertyName}) failed: ${e.message}")
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

    def resolveSaleProfileId = { ProfileService ps, String brandValue, Map saleProps, String fallbackProfileId ->
        if (!ps) {
            return fallbackProfileId
        }
        def orderNumber = (saleProps?.get("orderNumber") ?: saleProps?.get("order_number"))?.toString()?.trim()
        if (orderNumber) {
            def byOrder = findProfilesByProperty(ps, "properties.orderNumber", orderNumber)
            def bestOrder = pickNewestProfile(byOrder)
            if (bestOrder?.itemId) {
                return bestOrder.itemId.toString().trim()
            }
        }

        def email = (saleProps?.get("billing_email") ?: saleProps?.get("email") ?: saleProps?.get("billingEmail"))?.toString()?.trim()?.toLowerCase()
        if (!email) {
            email = profile?.getProperty("email")?.toString()?.trim()?.toLowerCase()
        }
        if (email) {
            def scopeEmail = (saleProps?.get("scopeEmail") ?: "${brandValue}-${email}").toString().trim().toLowerCase()
            def candidates = [] as LinkedHashSet
            candidates.addAll(findProfilesByProperty(ps, "properties.scopeEmail", scopeEmail))
            candidates.addAll(findProfilesByProperty(ps, "properties.email", email))
            def best = pickNewestProfile(candidates)
            if (best?.itemId) {
                return best.itemId.toString().trim()
            }
        }
        return fallbackProfileId
    }

    if (profileService) {
        def resolvedId = resolveSaleProfileId(profileService, brand, props, sessionProfileId)
        if (resolvedId && resolvedId != sessionProfileId) {
            logger.info("[sale] Canonical profile resolved: session=${sessionProfileId} -> ${resolvedId}")
            profileId = resolvedId
            try { profile = profileService.load(profileId) } catch (Exception ignore) {}
        }
    }

    def loyaltyUrl = action.getParameterValues().get("loyaltyUrl")?.toString()?.trim()
    def loyaltyUser = action.getParameterValues().get("loyaltyUsername")?.toString()
    def loyaltyPass = action.getParameterValues().get("loyaltyPassword")?.toString()

    if (!loyaltyUrl) {
        logger.error("[sale] Missing loyaltyUrl parameter; cannot push sale event.")
        return EventService.NO_CHANGE
    }

    def endpoint = loyaltyUrl.endsWith("/") ? (loyaltyUrl + "transactions") : (loyaltyUrl + "/transactions")

    def profileEmail = profile?.getProperty("email")?.toString()?.trim()
    if (profileEmail) {
        if (!props.billing_email && !props.billingEmail) props.billing_email = profileEmail
        if (!props.email) props.email = profileEmail
    }
    if (!props.brand) props.brand = brand

    def payload = [
        itemId    : (event.getItemId() ?: UUID.randomUUID().toString()),
        brand     : brand,
        eventType : "sale",
        profileId : profileId.toString(),
        properties: props
    ]

    def postSale = {
        String jsonPayload = JsonOutput.toJson(payload)
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
            }

            HttpResponse resp = httpClient.execute(req)
            int code = resp.getStatusLine().getStatusCode()
            String body = resp.getEntity() ? EntityUtils.toString(resp.getEntity()) : ""
            def bodySnippet = body ? body.substring(0, Math.min(800, body.length())) : ""

            if (code >= 200 && code < 300) {
                def txStatus = null
                try {
                    txStatus = body ? new JsonSlurper().parseText(body)?.status?.toString() : null
                } catch (Exception ignore) {}
                logger.info("[sale] Pushed to Loyalty. brand=${brand} profileId=${payload.profileId} code=${code} status=${txStatus ?: 'unknown'}")
            } else {
                logger.error("[sale] Push failed. brand=${brand} profileId=${payload.profileId} code=${code} body=${bodySnippet}")
            }
            return code
        } catch (Exception e) {
            logger.error("[sale] Error pushing sale. brand=${brand} profileId=${payload.profileId}", e)
            return 0
        } finally {
            try { httpClient.close() } catch (Exception ignore) {}
        }
    }

    postSale()

    if (profileService) {
        try {
            Thread.sleep(2500)
            def lateId = resolveSaleProfileId(profileService, brand, props, profileId)
            if (lateId && lateId != profileId) {
                logger.info("[sale] Post-merge reconcile: ${profileId} -> ${lateId}")
                payload.profileId = lateId
                postSale()
            }
        } catch (Exception ignore) {}
    }

    return EventService.NO_CHANGE
}
