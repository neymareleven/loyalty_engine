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

final Logger logger = LoggerFactory.getLogger("sale_to_loyalty_engine")

@Action(id = "sale", actionExecutor = "groovy:sale", parameters = [
    @Parameter(id = "loyaltyUrl", type = "string", multivalued = false),
    @Parameter(id = "loyaltyUsername", type = "string", multivalued = false),
    @Parameter(id = "loyaltyPassword", type = "string", multivalued = false)
])
def execute() {
    def eventType = null
    try {
        eventType = event.getEventType()
    } catch (Exception ignore) {
        // ignore
    }

    if (!eventType || eventType != "sale") {
        return EventService.NO_CHANGE
    }

    Profile profile = null
    try {
        profile = event.getProfile()
    } catch (Exception ignore) {
        // ignore
    }

    def profileId = null
    try {
        profileId = event.getProfileId()
    } catch (Exception ignore) {
        profileId = profile?.getItemId()
    }

    if (!profileId || !profileId.toString().trim()) {
        logger.warn("Missing profileId; skipping sale push.")
        return EventService.NO_CHANGE
    }

    def props = [:]
    try {
        props = event.getProperties() ?: [:]
    } catch (Exception ignore) {
        props = [:]
    }

    // Brand: first from event properties (your sample has properties.brand), else from profile property "brand"
    def brand = (props?.get("brand") ?: profile?.getProperty("brand"))?.toString()
    if (!brand || !brand.trim()) {
        logger.warn("Missing brand in sale event; skipping. profileId=${profileId}")
        return EventService.NO_CHANGE
    }

    def loyaltyUrl = action.getParameterValues().get("loyaltyUrl")?.toString()?.trim()
    def loyaltyUser = action.getParameterValues().get("loyaltyUsername")?.toString()
    def loyaltyPass = action.getParameterValues().get("loyaltyPassword")?.toString()

    if (!loyaltyUrl) {
        logger.error("Missing loyaltyUrl parameter; cannot push sale event.")
        return EventService.NO_CHANGE
    }

    def endpoint = loyaltyUrl.endsWith("/") ? (loyaltyUrl + "transactions") : (loyaltyUrl + "/transactions")

    // Loyalty Engine expects UnomiEventCreate:
    // { itemId, brand, eventType, profileId, properties }
    def payload = [
        itemId    : (event.getItemId() ?: UUID.randomUUID().toString()),
        brand     : brand,
        eventType : "sale",
        profileId : profileId.toString(),
        properties: props
    ]

    String jsonPayload = JsonOutput.toJson(payload)

    CloseableHttpClient httpClient = HttpClientBuilder.create().build()
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

        if (code >= 200 && code < 300) {
            logger.info("Sale pushed to Loyalty Engine. brand=${brand} profileId=${profileId} code=${code}")
        } else {
            logger.error("Sale push failed. brand=${brand} profileId=${profileId} code=${code} body=${body?.substring(0, Math.min(800, body.length()))}")
        }
    } catch (Exception e) {
        logger.error("Error pushing sale to Loyalty Engine. brand=${brand} profileId=${profileId}", e)
    } finally {
        try { httpClient.close() } catch (Exception ignore) {}
    }

    return EventService.NO_CHANGE
}
