/**
 * Action Unomi : notifier le loyalty engine quand un profil est supprimé côté CDP.
 * Déployer comme loyalty_profile.groovy puis brancher sur une règle privacy / profileDeleted.
 */
import org.apache.unomi.api.services.EventService
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.entity.ContentType
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.util.EntityUtils
import groovy.json.JsonOutput
import org.slf4j.LoggerFactory

final logger = LoggerFactory.getLogger("loyalty_profile_deleted")

@Action(id = "loyalty_profile_deleted", actionExecutor = "groovy:loyalty_profile_deleted", parameters = [
    @Parameter(id = "loyaltyUrl", type = "string", multivalued = false),
    @Parameter(id = "loyaltyUsername", type = "string", multivalued = false),
    @Parameter(id = "loyaltyPassword", type = "string", multivalued = false),
    @Parameter(id = "webhookSecret", type = "string", multivalued = false),
])
def execute() {
    def profileId = null
    try { profileId = event?.getProfileId()?.toString()?.trim() } catch (ignore) {}
    if (!profileId) {
        try { profileId = profile?.getItemId()?.toString()?.trim() } catch (ignore) {}
    }
    if (!profileId) {
        logger.warn("[loyalty_profile_deleted] missing profileId, skip")
        return EventService.NO_CHANGE
    }

    def brand = null
    try {
        def scope = event?.getScope()?.toString()?.trim()
        if (scope && scope != "systemscope") brand = scope
    } catch (ignore) {}
    if (!brand) {
        try { brand = profile?.getProperty("brand")?.toString()?.trim() } catch (ignore) {}
    }
    if (!brand) {
        logger.warn("[loyalty_profile_deleted] missing brand/scope for profileId=${profileId}, skip")
        return EventService.NO_CHANGE
    }

    def loyaltyUrl = action.getParameterValues().get("loyaltyUrl")?.toString()?.trim()
    def loyaltyUser = action.getParameterValues().get("loyaltyUsername")?.toString()
    def loyaltyPass = action.getParameterValues().get("loyaltyPassword")?.toString()
    def webhookSecret = action.getParameterValues().get("webhookSecret")?.toString()

    if (!loyaltyUrl) {
        logger.error("[loyalty_profile_deleted] loyaltyUrl not configured")
        return EventService.NO_CHANGE
    }

    def endpoint = loyaltyUrl.endsWith("/")
        ? (loyaltyUrl + "integrations/unomi/profile-events")
        : (loyaltyUrl + "/integrations/unomi/profile-events")

    def body = JsonOutput.toJson([
        event     : "profile_deleted",
        brand     : brand,
        profileId : profileId,
        scope     : brand,
    ])

    def client = HttpClientBuilder.create().build()
    try {
        def req = new HttpPost(endpoint)
        req.setEntity(new StringEntity(body, ContentType.APPLICATION_JSON))
        req.addHeader("Content-Type", "application/json")
        req.addHeader("Accept", "application/json")
        if (loyaltyUser != null && loyaltyPass != null) {
            req.addHeader("Authorization", "Basic " + "${loyaltyUser}:${loyaltyPass}".bytes.encodeBase64().toString())
        }
        if (webhookSecret) {
            req.addHeader("X-Unomi-Webhook-Secret", webhookSecret.toString())
        }
        def resp = client.execute(req)
        def code = resp.getStatusLine().getStatusCode()
        def respBody = resp.getEntity() ? EntityUtils.toString(resp.getEntity()) : ""
        if (code >= 200 && code < 300) {
            logger.info("[loyalty_profile_deleted] ok brand=${brand} profileId=${profileId} code=${code}")
        } else {
            logger.error("[loyalty_profile_deleted] failed code=${code} body=${respBody?.take(500)}")
        }
    } catch (Exception e) {
        logger.error("[loyalty_profile_deleted] error brand=${brand} profileId=${profileId}", e)
    } finally {
        try { client.close() } catch (ignore) {}
    }

    return EventService.NO_CHANGE
}
