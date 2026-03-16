import org.apache.unomi.api.Profile
import org.apache.unomi.api.services.ProfileService
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.unomi.api.services.EventService
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.util.EntityUtils
import org.apache.http.client.ClientProtocolException
import org.apache.http.HttpResponse
import org.apache.http.entity.ContentType
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.HashMap
import groovy.json.JsonOutput
import groovy.json.JsonSlurper

final Logger logger = LoggerFactory.getLogger("lesjardinsderene")

@Action(id = "lesjardinsderene", actionExecutor = "groovy:lesjardinsderene", parameters = [
    @Parameter(id = "mauticUrl", type = "string", multivalued = false),
    @Parameter(id = "mauticUsername", type = "string", multivalued = false),
    @Parameter(id = "mauticPassword", type = "string", multivalued = false)
])
def execute() {
    Profile profile = event.getProfile()

    // Extract profile properties
    def email = profile.getProperty("email")
    def brand = profile.getProperty("brand")
    def orderDate = profile.getProperty("orderDate")
    def firstName = profile.getProperty("firstName")
    def lastName = profile.getProperty("lastName")
    def age = profile.getProperty("age")
    def lastVisit = profile.getProperty("lastVisit")
    def phoneNumber = profile.getProperty("phoneNumber")
    def phone = profile.getProperty("phone")
    def birthDate = profile.getProperty("birthDate")
    def siteMessage = profile.getProperty("site-message")
    def gender = profile.getProperty("gender")
    def language = profile.getProperty("language")
    def levelOfSatisfaction = profile.getProperty("levelOfSatisfaction")
    def pageViewCount = profile.getProperty("pageViewCount")
    def nbOfVisits = profile.getProperty("nbOfVisits")
    def formOfComunication = profile.getProperty("formOfComunication")
    def jobTitle = profile.getProperty("jobTitle")
    def maritalStatus = profile.getProperty("maritalStatus")
    def kids = profile.getProperty("kids")
    def levelOfEducation = profile.getProperty("levelOfEducation")
    def scopeEmail = profile.getProperty("scopeEmail")
    def pushId = profile.getProperty("pushId")
    def pushEnabled = profile.getProperty("pushEnabled")
    def isAnonymous = profile.getProperty("isAnonymous")
    def doNotContactFlag = profile.getProperty("doNotContact")
    def whatsappWaId = profile.getProperty("whatsappWaId")
    def facebookId = profile.getProperty("facebookId")
    def instagramId = profile.getProperty("instagramId")
    def profileId = profile.getItemId()
    
    // Extract sessionId from event
    def sessionId = null
    try {
        sessionId = event.getSessionId()
    } catch (Exception e) {
        logger.debug("Could not get sessionId from event: ${e.message}")
        // Fallback: try to get from profile property
        sessionId = profile.getProperty("sessionId")
    }
    
    // Extract system properties
    def systemProperties = profile.getSystemProperties()
    def lastUpdated = systemProperties?.get("lastUpdated")

    logger.info("[lesjardinsderene ACTION] Processing profile: ${profileId}, Email: ${email}, Brand: ${brand}")

    // Check if profile has email and correct brand before processing
    if (email != null && !email.isEmpty() && brand != null && brand.equals("lesjardinsderene")) {
        def mauticUrl = action.getParameterValues().get("mauticUrl")
        // Use /api/contacts/new endpoint - it will create if new, update if exists (based on email)
        def mauticEndpoint = "${mauticUrl}/api/contacts/new"

        def unomiUrl = "https://cdp.qilinsa.com:9443/cxs/events/search"
        def unomiEndpoint = unomiUrl

        def unomiPayload = JsonOutput.toJson([
            condition: [
                type: "profilePropertyCondition",
                parameterValues: [
                    propertyName: "profileId",
                    comparisonOperator: "equals",
                    propertyValue: profileId
                ]
            ],
            sortby: "timeStamp:desc",
            offset: 0,
            limit: 1000
        ])

        CloseableHttpClient httpClient = HttpClientBuilder.create().build()
        try {
            HttpPost unomiRequest = new HttpPost(unomiEndpoint)
            StringEntity unomiEntity = new StringEntity(unomiPayload, ContentType.APPLICATION_JSON)
            unomiRequest.setEntity(unomiEntity)

            def unomiAuthHeader = "karaf:karaf".bytes.encodeBase64().toString()
            unomiRequest.addHeader("Authorization", "Basic " + unomiAuthHeader)

            HttpResponse unomiResponse = httpClient.execute(unomiRequest)
            String unomiResponseBody = EntityUtils.toString(unomiResponse.getEntity())
            def unomiResponseJson = new JsonSlurper().parseText(unomiResponseBody)

            logger.info("Unomi response received. Total events: ${unomiResponseJson.list?.size() ?: 0}")

            int totalNumberOfOrders = 0
            int totalNumberOfProducts = 0
            double totalSalesAmount = 0.0

            Map<String, Integer> pagePathCount = [:]  // To store extracted page paths and their visit counts
            String mostVisitedPagePath = ""
            int maxVisits = 0

            String lastViewedProduct = ""  // To store the last viewed product

            if (unomiResponseJson?.list) {
                unomiResponseJson.list.each { event ->
                    logger.debug("Processing event: ${event.itemId}")

                    if (event.eventType == "sale") {
                        def productQuantities = event.properties?.productQuantities
                        def productSubtotals = event.properties?.productSubtotals
                        def orderTotal = event.properties?.orderTotal

                        if (productQuantities && productSubtotals && orderTotal) {
                            totalNumberOfOrders++

                            productQuantities.each { quantity ->
                                try {
                                    totalNumberOfProducts += quantity.toInteger()
                                } catch (Exception e) {
                                    logger.warn("Error parsing quantity: ${quantity}", e)
                                }
                            }

                            try {
                                def cleanOrderTotal = orderTotal.toString().replaceAll("[^\\d.]", "")
                                totalSalesAmount += cleanOrderTotal.toDouble()
                            } catch (Exception e) {
                                logger.warn("Error parsing order total: ${orderTotal}", e)
                            }
                        } else {
                            logger.warn("Sale event ${event.itemId} is missing required data")
                        }
                    }

                    // Handle 'view' events to track page paths
                    if (event.eventType == "view") {
                        def pagePath = event?.target?.properties?.pageInfo?.pagePath
                        if (pagePath && pagePath.startsWith("/produit/")) { // Only process paths starting with /produit/
                            def extractedPath = pagePath.replaceFirst("/produit/", "") // Extract the segment after /produit/
                            if (extractedPath.contains("/")) {
                                extractedPath = extractedPath.split("/")[0] // Get only the next segment
                            }

                            // Update last viewed product (since events are sorted by time descending)
                            if (lastViewedProduct == "") {
                                lastViewedProduct = extractedPath
                            }

                            pagePathCount[extractedPath] = pagePathCount.getOrDefault(extractedPath, 0) + 1
                            if (pagePathCount[extractedPath] > maxVisits) {
                                mostVisitedPagePath = extractedPath
                                maxVisits = pagePathCount[extractedPath]
                            }
                        }
                    }
                }
            } else {
                logger.warn("No events found for profile ${profileId}")
            }

            logger.info("Calculations complete. Orders: ${totalNumberOfOrders}, Products: ${totalNumberOfProducts}, Total Sales: ${totalSalesAmount}, Most visited product: ${mostVisitedPagePath}")
            double averageSpendingPerOrder = totalNumberOfOrders > 0 ? totalSalesAmount / totalNumberOfOrders : 0.0

            // Build the payload to send to Mautic
            HashMap<String, Object> payload = new HashMap<>()
            payload.put("email", email)
            payload.put("unomi_profile_id", profileId)  // Renamed to avoid Mautic number validation
            if (brand != null && !brand.trim().isEmpty()) payload.put("brand", brand)
            
            // Add optional profile fields only if they have values
            if (orderDate != null) payload.put("orderdate", orderDate)
            if (firstName != null && !firstName.trim().isEmpty()) payload.put("firstname", firstName)
            if (lastName != null && !lastName.trim().isEmpty()) payload.put("lastname", lastName)
            if (phoneNumber != null && !phoneNumber.trim().isEmpty()) payload.put("phone", phoneNumber)
            if (phone != null && !phone.trim().isEmpty()) payload.put("phone", phone)
            if (birthDate != null) payload.put("birthdate", birthDate)
            if (kids != null) payload.put("kids", kids)
            if (instagramId != null) payload.put("instagramid", instagramId)
            if (facebookId != null) payload.put("facebookid", facebookId)
            if (siteMessage != null) payload.put("message", siteMessage)
            if (whatsappWaId != null) payload.put("whatsappwaid", whatsappWaId)
            if (levelOfEducation != null && !levelOfEducation.trim().isEmpty()) payload.put("levelOfEducation", levelOfEducation)
            if (gender != null && !gender.trim().isEmpty()) payload.put("gender", gender)
            if (age != null) payload.put("age", age)
            if (language != null && !language.trim().isEmpty()) payload.put("language", language)
            if (levelOfSatisfaction != null) payload.put("satisfactionlevel", levelOfSatisfaction)
            if (pageViewCount != null) payload.put("pageviews", pageViewCount)
            if (nbOfVisits != null) payload.put("totalnumberofvisits", nbOfVisits)
            if (formOfComunication != null && !formOfComunication.trim().isEmpty()) payload.put("commuication", formOfComunication)
            if (jobTitle != null && !jobTitle.trim().isEmpty()) payload.put("jobtitle", jobTitle)
            if (maritalStatus != null && !maritalStatus.trim().isEmpty()) payload.put("maritalstatus", maritalStatus)
            if (lastVisit != null) payload.put("last_active", lastVisit)
            if (lastUpdated != null) payload.put("last_updated", lastUpdated)
            payload.put("totalorders", totalNumberOfOrders)
            payload.put("totalproducts", totalNumberOfProducts)
            payload.put("totalsalesamount", totalSalesAmount)
            payload.put("averagespendingperorder", averageSpendingPerOrder)
            payload.put("mostviewedproduct", mostVisitedPagePath)
            payload.put("lastproductviews", lastViewedProduct)
            if (scopeEmail != null && !scopeEmail.trim().isEmpty()) payload.put("scopeemail", scopeEmail)
            if (sessionId != null && !sessionId.trim().isEmpty()) payload.put("sessionid", sessionId)
            if (pushId != null && !pushId.trim().isEmpty()) payload.put("pushid", pushId)
            if (pushEnabled != null) payload.put("pushenabled", pushEnabled)
            if (isAnonymous != null) payload.put("isanonymous", isAnonymous)

            // Handle Mautic doNotContact structure based on Unomi doNotContact flag
            if (doNotContactFlag != null) {
                boolean doNotContactBool = (doNotContactFlag instanceof Boolean) ? doNotContactFlag : doNotContactFlag.toString().toBoolean()
                int reason = doNotContactBool ? 3 : 0
                def dncEntry = [
                    channel : "email",
                    reason  : reason,
                    comments: "Set to DNC upon creation"
                ]
                payload.put("doNotContact", [dncEntry])
            }

            // --- Start of Tag Handling ---
            // Extract segments from the profile to use as tags - comprehensive approach
            def allSegments = []
            
            // Method 1: Try profile.getSegments() - direct method to get segments
            try {
                def profileSegments = profile.getSegments()
                if (profileSegments) {
                    if (profileSegments instanceof Collection) {
                        allSegments.addAll(profileSegments)
                        logger.debug("Found ${profileSegments.size()} segments via getSegments() method")
                    } else if (profileSegments instanceof String) {
                        allSegments.add(profileSegments)
                        logger.debug("Found 1 segment (String) via getSegments() method")
                    }
                }
            } catch (Exception e) {
                logger.debug("getSegments() method not available or failed: ${e.message}")
            }
            
            // Method 2: Try profile.getProperty("segments")
            try {
                def profileSegments = profile.getProperty("segments")
                if (profileSegments) {
                    if (profileSegments instanceof Collection) {
                        allSegments.addAll(profileSegments)
                        logger.debug("Found segments via getProperty('segments')")
                    } else if (profileSegments instanceof String) {
                        allSegments.add(profileSegments)
                        logger.debug("Found segment (String) via getProperty('segments')")
                    }
                }
            } catch (Exception e) {
                logger.debug("getProperty('segments') not available or failed: ${e.message}")
            }
            
            // Method 3: Check in profile properties map (some Unomi versions store segments there)
            try {
                def profileProperties = profile.getProperties()
                if (profileProperties && profileProperties.containsKey("segments")) {
                    def propsSegments = profileProperties.get("segments")
                    if (propsSegments) {
                        if (propsSegments instanceof Collection) {
                            allSegments.addAll(propsSegments)
                            logger.debug("Found segments in profile properties map")
                        } else if (propsSegments instanceof String) {
                            allSegments.add(propsSegments)
                            logger.debug("Found segment (String) in profile properties map")
                        }
                    }
                }
            } catch (Exception e) {
                logger.debug("Could not access segments from properties map: ${e.message}")
            }

            def tags = []

            // Process segments into tags with robust handling
            if (allSegments && !allSegments.isEmpty()) {
                allSegments.each { seg ->
                    if (seg != null) {
                        if (seg instanceof String && !seg.trim().isEmpty()) {
                            tags << seg.trim()
                        } else if (seg instanceof Map && seg.containsKey("id")) {
                            // Handle segments that are maps with 'id' field
                            def segmentId = seg.get("id")
                            if (segmentId && !segmentId.toString().trim().isEmpty()) {
                                tags << segmentId.toString().trim()
                            }
                        } else if (seg instanceof Map && seg.containsKey("name")) {
                            // Handle segments that are maps with 'name' field
                            def segmentName = seg.get("name")
                            if (segmentName && !segmentName.toString().trim().isEmpty()) {
                                tags << segmentName.toString().trim()
                            }
                        }
                    }
                }
                
                // Remove duplicates and empty strings
                tags = tags.findAll { it != null && !it.trim().isEmpty() }
                tags = tags.unique()
                
                logger.info("Found ${allSegments.size()} segments for profile ${profileId}, processed into ${tags.size()} unique tags: ${tags}")
            } else {
                logger.info("No segments found for profile ${profileId}. This might indicate removal from all segments.")
            }

            // Always add tags to payload as an array (even if empty) to ensure segment removals are properly synchronized
            // Mautic expects tags as an array of strings
            payload.put("tags", tags ?: [])
            
            if (tags && !tags.isEmpty()) {
                logger.info("Sending ${tags.size()} tags to Mautic for profile ${profileId}: ${tags}")
            } else {
                logger.info("Sending empty tags list to Mautic for profile ${profileId} - will remove all existing tags")
            }
            // --- End of Tag Handling ---

            String jsonPayload = JsonOutput.toJson(payload)
            
            // Log the payload structure to verify tags are included
            def payloadKeys = payload.keySet()
            logger.info("Preparing to send payload to Mautic with ${payloadKeys.size()} fields. Tags included: ${payload.containsKey('tags') ? 'Yes (' + (payload.get('tags')?.size() ?: 0) + ' tags)' : 'No'}")
            if (payload.containsKey('tags') && payload.get('tags')) {
                logger.info("Tags in payload: ${payload.get('tags')}")
            }

            HttpPost request = new HttpPost(mauticEndpoint)
            StringEntity entity = new StringEntity(jsonPayload, ContentType.APPLICATION_JSON)
            request.setEntity(entity)

            def mauticAuthHeader = "${action.getParameterValues().get("mauticUsername")}:${action.getParameterValues().get("mauticPassword")}".bytes.encodeBase64().toString()
            request.addHeader("Authorization", "Basic " + mauticAuthHeader)
            request.addHeader("Content-Type", "application/json")
            request.addHeader("Accept", "application/json")

            HttpResponse response = httpClient.execute(request)
            int responseCode = response.getStatusLine().getStatusCode()
            String responseBody = EntityUtils.toString(response.getEntity())

            if (responseCode == 200 || responseCode == 201) {
                logger.info("Profile ${profileId} successfully synced to Mautic with ${tags?.size() ?: 0} tags. Response code: ${responseCode}")
                // Log a sample of the payload for debugging (without sensitive data)
                def samplePayload = [
                    email: payload.get("email"),
                    firstname: payload.get("firstname"),
                    lastname: payload.get("lastname"),
                    tags: payload.get("tags")
                ]
                logger.debug("Sample payload sent: ${JsonOutput.toJson(samplePayload)}")
            } else {
                logger.error("Failed to sync profile ${profileId} to Mautic. Response code: ${responseCode}, Response body: ${responseBody?.substring(0, Math.min(500, responseBody?.length() ?: 0))}")
            }
        } catch (ClientProtocolException e) {
            logger.error("Client protocol exception occurred while syncing profile ${profileId} to Mautic", e)
        } catch (IOException e) {
            logger.error("IO exception occurred while syncing profile ${profileId} to Mautic", e)
        } catch (Exception e) {
            logger.error("Unexpected error occurred while processing profile ${profileId}", e)
        } finally {
            httpClient.close()
        }
    } else {
        if (email == null || email.isEmpty()) {
            logger.warn("[lesjardinsderene ACTION] Profile ${profileId} does not contain an email address, skipping sync.")
        } else if (brand == null || !brand.equals("lesjardinsderene")) {
            logger.warn("[lesjardinsderene ACTION] Profile ${profileId} does not have brand 'lesjardinsderene' (current brand: ${brand}), skipping sync.")
        } else {
            logger.warn("[lesjardinsderene ACTION] Profile ${profileId} does not meet sync criteria, skipping sync.")
        }
    }

    return EventService.NO_CHANGE
}
