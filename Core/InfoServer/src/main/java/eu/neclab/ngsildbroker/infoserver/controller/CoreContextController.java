package eu.neclab.ngsildbroker.infoserver.controller;

import javax.servlet.http.HttpServletRequest;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/corecontext")
public class CoreContextController {
	

	private final String CORE_CONTEXT = "{\r\n" + 
			"    \"@context\": {\r\n" + 
			"      \"ngsi-ld\": \"https://uri.etsi.org/ngsi-ld/\",    \r\n" + 
			"      \"id\": \"@id\",\r\n" + 
			"      \"type\": \"@type\",\r\n" + 
			"      \"value\": \"https://uri.etsi.org/ngsi-ld/hasValue\",\r\n" + 
			"      \"object\": {\r\n" + 
			"        \"@id\": \"https://uri.etsi.org/ngsi-ld/hasObject\",\r\n" + 
			"        \"@type\":\"@id\"\r\n" + 
			"      },\r\n" + 
			"      \"Property\": \"https://uri.etsi.org/ngsi-ld/Property\",\r\n" + 
			"      \"Relationship\": \"https://uri.etsi.org/ngsi-ld/Relationship\",\r\n" + 
			"      \"DateTime\": \"https://uri.etsi.org/ngsi-ld/DateTime\",\r\n" + 
			"      \"Date\": \"https://uri.etsi.org/ngsi-ld/Date\",\r\n" + 
			"      \"Time\": \"https://uri.etsi.org/ngsi-ld/Time\",\r\n" + 
			"      \"createdAt\": {\r\n" + 
			"        \"@id\": \"https://uri.etsi.org/ngsi-ld/createdAt\",\r\n" + 
			"        \"@type\": \"DateTime\"\r\n" + 
			"      },\r\n" + 
			"      \"modifiedAt\": {\r\n" + 
			"        \"@id\": \"https://uri.etsi.org/ngsi-ld/modifiedAt\",\r\n" + 
			"        \"@type\": \"DateTime\"\r\n" + 
			"      },\r\n" + 
			"      \"observedAt\": {\r\n" + 
			"        \"@id\": \"https://uri.etsi.org/ngsi-ld/observedAt\",\r\n" + 
			"        \"@type\": \"DateTime\"\r\n" + 
			"      },\r\n" + 
			"      \"datasetId\": {\r\n" + 
			"        \"@id\": \"https://uri.etsi.org/ngsi-ld/datasetId\",\r\n" + 
			"        \"@type\": \"@id\"\r\n" + 
			"      },\r\n" + 
			"      \"instanceId\": {\r\n" + 
			"        \"@id\": \"https://uri.etsi.org/ngsi-ld/instanceId\",\r\n" + 
			"        \"@type\": \"@id\"\r\n" + 
			"      },\r\n" + 
			"      \"unitCode\": \"https://uri.etsi.org/ngsi-ld/unitCode\",\r\n" + 
			"      \"location\": \"https://uri.etsi.org/ngsi-ld/location\",\r\n" + 
			"      \"observationSpace\": \"https://uri.etsi.org/ngsi-ld/observationSpace\",\r\n" + 
			"      \"operationSpace\": \"https://uri.etsi.org/ngsi-ld/operationSpace\",\r\n" + 
			"      \"GeoProperty\": \"https://uri.etsi.org/ngsi-ld/GeoProperty\",\r\n" + 
			"      \"TemporalProperty\": \"https://uri.etsi.org/ngsi-ld/TemporalProperty\",\r\n" + 
			"      \"ContextSourceRegistration\": \"https://uri.etsi.org/ngsi-ld/ContextSourceRegistration\",\r\n" + 
			"      \"Subscription\": \"https://uri.etsi.org/ngsi-ld/Subscription\", \r\n" + 
			"      \"Notification\": \"https://uri.etsi.org/ngsi-ld/Notification\",\r\n" + 
			"      \"ContextSourceNotification\": \"https://uri.etsi.org/ngsi-ld/ContextSourceNotification\",\r\n" + 
			"      \"title\": \"https://uri.etsi.org/ngsi-ld/title\",\r\n" + 
			"      \"detail\": \"https://uri.etsi.org/ngsi-ld/detail\",\r\n" + 
			"      \"idPattern\": \"https://uri.etsi.org/ngsi-ld/idPattern\",\r\n" + 
			"      \"name\": \"https://uri.etsi.org/ngsi-ld/name\",\r\n" + 
			"      \"description\": \"https://uri.etsi.org/ngsi-ld/description\",\r\n" + 
			"      \"information\": \"https://uri.etsi.org/ngsi-ld/information\",\r\n" + 
			"      \"observationInterval\": \"https://uri.etsi.org/ngsi-ld/observationInterval\",\r\n" + 
			"      \"managementInterval\": \"https://uri.etsi.org/ngsi-ld/managementInterval\",\r\n" + 
			"      \"expires\": {\r\n" + 
			"        \"@id\": \"https://uri.etsi.org/ngsi-ld/expires\",\r\n" + 
			"        \"@type\": \"DateTime\"\r\n" + 
			"      },\r\n" + 
			"      \"endpoint\": \"https://uri.etsi.org/ngsi-ld/endpoint\",\r\n" + 
			"      \"entities\": \"https://uri.etsi.org/ngsi-ld/entities\",\r\n" + 
			"      \"properties\": {\r\n" + 
			"        \"@id\": \"https://uri.etsi.org/ngsi-ld/properties\",\r\n" + 
			"        \"@type\": \"@vocab\"\r\n" + 
			"      },\r\n" + 
			"      \"relationships\": {\r\n" + 
			"        \"@id\": \"https://uri.etsi.org/ngsi-ld/relationships\",\r\n" + 
			"        \"@type\": \"@vocab\"\r\n" + 
			"      },\r\n" + 
			"      \"start\": {\r\n" + 
			"        \"@id\": \"https://uri.etsi.org/ngsi-ld/start\",\r\n" + 
			"        \"@type\": \"DateTime\"\r\n" + 
			"      },\r\n" + 
			"      \"end\": {\r\n" + 
			"        \"@id\": \"https://uri.etsi.org/ngsi-ld/end\",\r\n" + 
			"        \"@type\": \"DateTime\"\r\n" + 
			"      },\r\n" + 
			"      \"watchedAttributes\":{\r\n" + 
			"        \"@id\": \"https://uri.etsi.org/ngsi-ld/watchedAttributes\",\r\n" + 
			"        \"@type\": \"@vocab\"\r\n" + 
			"      },\r\n" + 
			"      \"timeInterval\": \"https://uri.etsi.org/ngsi-ld/timeInterval\",\r\n" + 
			"      \"q\": \"https://uri.etsi.org/ngsi-ld/q\",\r\n" + 
			"      \"geoQ\": \"https://uri.etsi.org/ngsi-ld/geoQ\",\r\n" + 
			"      \"csf\": \"https://uri.etsi.org/ngsi-ld/csf\",\r\n" + 
			"      \"isActive\": \"https://uri.etsi.org/ngsi-ld/isActive\",\r\n" + 
			"      \"notification\": \"https://uri.etsi.org/ngsi-ld/notification\",\r\n" + 
			"      \"status\": \"https://uri.etsi.org/ngsi-ld/status\",\r\n" + 
			"      \"throttling\": \"https://uri.etsi.org/ngsi-ld/throttling\",\r\n" + 
			"      \"temporalQ\": \"https://uri.etsi.org/ngsi-ld/temporalQ\",\r\n" + 
			"      \"geometry\": \"https://uri.etsi.org/ngsi-ld/geometry\",\r\n" + 
			"      \"coordinates\": \"https://uri.etsi.org/ngsi-ld/coordinates\",\r\n" + 
			"      \"georel\": \"https://uri.etsi.org/ngsi-ld/georel\",\r\n" + 
			"      \"geoproperty\": \"https://uri.etsi.org/ngsi-ld/geoproperty\",\r\n" + 
			"      \"attributes\": {\r\n" + 
			"        \"@id\": \"https://uri.etsi.org/ngsi-ld/attributes\",\r\n" + 
			"        \"@type\": \"@vocab\"\r\n" + 
			"      },\r\n" + 
			"      \"format\": \"https://uri.etsi.org/ngsi-ld/format\",\r\n" + 
			"      \"timesSent\": \"https://uri.etsi.org/ngsi-ld/timesSent\",\r\n" + 
			"      \"lastNotification\":{\r\n" + 
			"        \"@id\": \"https://uri.etsi.org/ngsi-ld/lastNotification\",\r\n" + 
			"        \"@type\": \"DateTime\"\r\n" + 
			"      },\r\n" + 
			"      \"lastFailure\":{\r\n" + 
			"        \"@id\": \"https://uri.etsi.org/ngsi-ld/lastFailure\",\r\n" + 
			"        \"@type\": \"DateTime\"\r\n" + 
			"      },\r\n" + 
			"      \"lastSuccess\":{\r\n" + 
			"        \"@id\": \"https://uri.etsi.org/ngsi-ld/lastSuccess\",\r\n" + 
			"        \"@type\": \"DateTime\"\r\n" + 
			"      },\r\n" + 
			"      \"uri\": \"https://uri.etsi.org/ngsi-ld/uri\",\r\n" + 
			"      \"accept\": \"https://uri.etsi.org/ngsi-ld/accept\",\r\n" + 
			"      \"success\": {\r\n" + 
			"        \"@id\": \"https://uri.etsi.org/ngsi-ld/success\",\r\n" + 
			"        \"@type\": \"@id\"\r\n" + 
			"      },\r\n" + 
			"      \"errors\": \"https://uri.etsi.org/ngsi-ld/errors\",\r\n" + 
			"      \"error\": \"https://uri.etsi.org/ngsi-ld/error\",\r\n" + 
			"      \"entityId\": {\r\n" + 
			"        \"@id\": \"https://uri.etsi.org/ngsi-ld/entityId\",\r\n" + 
			"        \"@type\": \"@id\"\r\n" + 
			"      },\r\n" + 
			"      \"updated\": \"https://uri.etsi.org/ngsi-ld/updated\",\r\n" + 
			"      \"unchanged\": \"https://uri.etsi.org/ngsi-ld/unchanged\",\r\n" + 
			"      \"attributeName\": \"https://uri.etsi.org/ngsi-ld/attributeName\",\r\n" + 
			"      \"reason\": \"https://uri.etsi.org/ngsi-ld/reason\",\r\n" + 
			"      \"timerel\": \"https://uri.etsi.org/ngsi-ld/timerel\",\r\n" + 
			"      \"time\": {\r\n" + 
			"        \"@id\": \"https://uri.etsi.org/ngsi-ld/time\",\r\n" + 
			"        \"@type\": \"DateTime\"\r\n" + 
			"      },\r\n" + 
			"      \"endTime\": {\r\n" + 
			"        \"@id\": \"https://uri.etsi.org/ngsi-ld/endTime\",\r\n" + 
			"        \"@type\": \"DateTime\"\r\n" + 
			"      },\r\n" + 
			"      \"timeproperty\": \"https://uri.etsi.org/ngsi-ld/timeproperty\",\r\n" + 
			"      \"subscriptionId\": {\r\n" + 
			"        \"@id\": \"https://uri.etsi.org/ngsi-ld/subscriptionId\",\r\n" + 
			"        \"@type\": \"@id\"\r\n" + 
			"      },\r\n" + 
			"      \"notifiedAt\":{\r\n" + 
			"        \"@id\": \"https://uri.etsi.org/ngsi-ld/notifiedAt\",\r\n" + 
			"        \"@type\": \"DateTime\"\r\n" + 
			"      },\r\n" + 
			"      \"data\": \"https://uri.etsi.org/ngsi-ld/data\",\r\n" + 
			"      \"triggerReason\": \"https://uri.etsi.org/ngsi-ld/triggerReason\",\r\n" + 
			"      \"values\":{\r\n" + 
			"          \"@id\": \"https://uri.etsi.org/ngsi-ld/hasValues\",\r\n" + 
			"          \"@container\": \"@list\"\r\n" + 
			"      },\r\n" + 
			"      \"objects\":{\r\n" + 
			"          \"@id\": \"https://uri.etsi.org/ngsi-ld/hasObjects\",\r\n" + 
			"	      \"@type\": \"@id\",\r\n" + 
			"	      \"@container\": \"@list\"\r\n" + 
			"      },\r\n" + 
			"      \"@vocab\": \"https://uri.etsi.org/ngsi-ld/default-context/\"\r\n" + 
			"    }\r\n" + 
			"}";
	@GetMapping
	public ResponseEntity<Object> getHealth(HttpServletRequest request) {
			return ResponseEntity.status(HttpStatus.ACCEPTED).header("Content-Type", "application/json")
				.body(CORE_CONTEXT);
	}
}
