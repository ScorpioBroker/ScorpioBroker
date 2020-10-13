package eu.neclab.ngsildbroker.commons.ldcontext;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.PostConstruct;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.core.RDFDataset;
import com.github.jsonldjava.core.RDFDatasetUtils;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.EndPoint;
import eu.neclab.ngsildbroker.commons.datatypes.EntityInfo;
import eu.neclab.ngsildbroker.commons.datatypes.GeoRelation;
import eu.neclab.ngsildbroker.commons.datatypes.LDGeoQuery;
import eu.neclab.ngsildbroker.commons.datatypes.LDQuery;
import eu.neclab.ngsildbroker.commons.datatypes.NotificationParam;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.enums.Format;
import eu.neclab.ngsildbroker.commons.enums.Geometry;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaOps;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;

@Component
public class ContextResolverBasic {
	private final static Logger logger = LogManager.getLogger(ContextResolverBasic.class);
	private URI CORE_CONTEXT_URL;
	@Value("${context.coreurl:https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld}")
	private String CORE_CONTEXT_URL_STR;
	
	private String USED_CORE_CONTEXT_URL_STR;
	// private URI DEFAULT_CONTEXT_URL;

	@Autowired
	KafkaOps kafkaOps;

	@Autowired
	AtContextProducerChannel producerChannel;

	private JsonLdOptions defaultOptions = new JsonLdOptions();

	@Value("${atcontext.baseurl:http://localhost:9090/ngsi-ld/contextes/}")
	private String AT_CONTEXT_BASE_URL;
	@Value("${selfhostcorecontext:http://localhost:9090/corecontext}")
	private String SELF_HOST_CORE_CONTEXT_URL;
	private HttpUtils httpUtils = HttpUtils.getInstance(this);
	private Map<String, Object> CORE_CONTEXT;
	// private Map<String, Object> DEFAULT_CONTEXT;
	private Map<String, Object> BASE_CONTEXT = new HashMap<String, Object>();
	Pattern attributeChecker;
	Pattern subscriptionParser;
	private static final String IS_FULL_VALID = "ajksd7868";

	@PostConstruct
	private void setup() {
		try {
			CORE_CONTEXT_URL = new URI(CORE_CONTEXT_URL_STR);
			String json = httpUtils.doGet(CORE_CONTEXT_URL);
			CORE_CONTEXT = (Map<String, Object>) ((Map) JsonUtils.fromString(json)).get("@context");
			BASE_CONTEXT.putAll(CORE_CONTEXT);
			USED_CORE_CONTEXT_URL_STR = CORE_CONTEXT_URL_STR;
		} catch (URISyntaxException e) {
			// left empty intentionally
			// controlled uri
			throw new AssertionError(
					CORE_CONTEXT_URL + " is not a valid uri. Aborting! core context has to be available");
		} catch (IOException e) {
			// core context not reachable
			try {
				CORE_CONTEXT_URL = new URI(SELF_HOST_CORE_CONTEXT_URL);
				String json = httpUtils.doGet(CORE_CONTEXT_URL);
				CORE_CONTEXT = (Map<String, Object>) ((Map) JsonUtils.fromString(json)).get("@context");
				BASE_CONTEXT.putAll(CORE_CONTEXT);
				USED_CORE_CONTEXT_URL_STR = SELF_HOST_CORE_CONTEXT_URL;
			} catch (URISyntaxException e1) {
				// left empty intentionally
				// controlled uri
				throw new AssertionError(SELF_HOST_CORE_CONTEXT_URL
						+ " is not a valid uri.  Aborting! core context has to be available");
			} catch (IOException e1) {
				throw new AssertionError(
						"Neither the default core context is reachable nore the internal webserver.  Aborting! core context has to be available");
			}
		}
	}

	public static void main(String[] args) throws Exception {
		ContextResolverBasic bla = new ContextResolverBasic();
		ArrayList<Object> contextLinks = new ArrayList<Object>();
		String body = "{ \"id\": \"urn:ngsi-ld:Building:store000000001\", \"type\": \"Building\", \"category\": { \"type\": \"Property\", \"value\": [\"commercial\"] }, \"address\": { \"type\": \"Property\", \"value\": { \"streetAddress\": \"Bornholmer Straße 65\", \"addressRegion\": \"Berlin\", \"addressLocality\": \"Prenzlauer Berg\", \"postalCode\": \"10439\" }, \"verified\": { \"type\": \"Property\", \"value\": true } }, \"location\": { \"type\": \"GeoProperty\", \"value\": { \"type\": \"Point\", \"coordinates\": [13.3986, 52.5547] } }, \"name\": { \"type\": \"Property\", \"value\": \"Bösebrücke Einkauf\" }, \"@context\": [ \"https://fiware.github.io/data-models/context.jsonld\", \"https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld\" ] }";
		/**/
	}

	public ContextResolverBasic(String atContextBaseUrl) {
		this();
		if (atContextBaseUrl != null) {
			this.AT_CONTEXT_BASE_URL = atContextBaseUrl;
		}
	}

	public ContextResolverBasic() {
		super();
		StringBuilder regex = new StringBuilder();
		regex.append(NGSIConstants.NGSI_LD_FORBIDDEN_KEY_CHARS_REGEX);
		for (String payloadItem : NGSIConstants.NGSI_LD_PAYLOAD_KEYS) {
			regex.append("|(" + payloadItem.replace("/", "\\/").replace(".", "\\.") + ")");
		}
		attributeChecker = Pattern.compile(regex.toString());
		regex = new StringBuilder();
		regex.append(NGSIConstants.NGSI_LD_FORBIDDEN_KEY_CHARS_REGEX);
		for (String payloadItem : NGSIConstants.NGSI_LD_SUBSCRIPTON_PAYLOAD_KEYS) {
			regex.append("|(" + payloadItem.replace("/", "\\/").replace(".", "\\.") + ")");
		}
		subscriptionParser = Pattern.compile(regex.toString());

	}

	public String expand(String body, List<Object> contextLinks, boolean check, int endPoint) throws ResponseException {
		try {
			Object obj = JsonUtils.fromString(body);
			if (obj instanceof Map) {
				return expand((Map<String, Object>) obj, contextLinks, check, endPoint);
			}
			if (obj instanceof List) {
				List<Object> list = (List<Object>) obj;
				if (list.isEmpty()) {
					throw new ResponseException(ErrorType.InvalidRequest);
				}
				StringBuilder result = new StringBuilder("[");
				for (Object listObj : list) {
					result.append(expand((Map<String, Object>) listObj, contextLinks, check, endPoint));
					result.append(",");
				}
				result.setCharAt(result.length() - 1, ']');
				return result.toString();
			}
			throw new ResponseException(ErrorType.InvalidRequest);
		} catch (IOException e) {
			e.printStackTrace();
			throw new ResponseException(ErrorType.InvalidRequest);
		}
	}

	private Map<Integer, List<Object>> expand(Map<String, Object> json, List<Object> contextLinks) {
		Object tempCtx = json.get(NGSIConstants.JSON_LD_CONTEXT);
		List<Object> context;
		if (tempCtx == null) {
			context = new ArrayList<Object>();
		} else if (tempCtx instanceof List) {
			context = (List<Object>) tempCtx;
		} else {
			context = new ArrayList<Object>();
			context.add(tempCtx);
		}

		if (contextLinks != null && !contextLinks.isEmpty()) {
			context.addAll(contextLinks);
		}
		ArrayList<Object> usedContext = new ArrayList<Object>();
		
		usedContext.addAll(context);
		usedContext.remove(CORE_CONTEXT_URL_STR);
		usedContext.add(BASE_CONTEXT);

		json.put(NGSIConstants.JSON_LD_CONTEXT, usedContext);

		Map<Integer, List<Object>> result = new HashMap<Integer, List<Object>>();
		result.put(1, JsonLdProcessor.expand(json));
		result.put(2, usedContext);
		return result;

	}

	public String expand(Map<String, Object> json, List<Object> contextLinks, boolean check, int endPoint)
			throws ResponseException {
		try {
			// if(!
			Map<Integer, List<Object>> expanded = expand(json, contextLinks);
			if (check) {
				preFlightCheck(expanded.get(1), expanded.get(2), true, endPoint, false);
			}
			// ) {
			// throw new ResponseException(ErrorType.BadRequestData,"Entity without an
			// attribute is not allowed");
			// }
//			protectGeoProps(expanded, usedContext);
//			protectLocationFromSubs(expanded, usedContext);
			if (expanded.get(1).isEmpty()) {
				return "";
			}
			return JsonUtils.toPrettyString(expanded.get(1).get(0));
		} catch (IOException e) {
			e.printStackTrace();
			throw new ResponseException(ErrorType.InvalidRequest, e.getLocalizedMessage());
		}

	}

	private boolean preFlightCheck(List<Object> expanded, List<Object> usedContext, boolean root, int calledEndpoint,
			boolean customKey) throws JsonGenerationException, ResponseException, IOException {
		boolean hasAttributes = false;
		for (Object entry : expanded) {
			if (entry instanceof Map) {
				hasAttributes = preFlightCheck((Map<String, Object>) entry, usedContext, root, calledEndpoint,
						customKey) || hasAttributes;
			} else if (entry instanceof List) {
				hasAttributes = preFlightCheck((List) entry, usedContext, root, calledEndpoint, customKey)
						|| hasAttributes;
			} else {
				// don't care for now i think
			}
		}
		return hasAttributes;
	}

	private boolean preFlightCheck(Map<String, Object> objMap, List<Object> usedContext, boolean root,
			int calledEndpoint, boolean customKey) throws ResponseException, JsonGenerationException, IOException {

		Object value = null;

		boolean hasValue = false;
		boolean hasObject = false;
		boolean hasAtValue = false;
		boolean hasAttributes = false;
		boolean isProperty = false;
		boolean isRelationship = false;
		boolean isDatetime = false;
		boolean isGeoProperty = false;
		int keyType;
		for (Entry<String, Object> mapEntry : objMap.entrySet()) {
			String key = mapEntry.getKey();
			Object mapValue = mapEntry.getValue();
			keyType = checkKey(key, attributeChecker);
			// (@id)|(@type)|(@context)|(https://uri.etsi.org/ngsi-ld/default-context/)|(https://uri.etsi.org/ngsi-ld/hasValue)|(https://uri.etsi.org/ngsi-ld/hasObject)|(https://uri.etsi.org/ngsi-ld/location)|(https://uri.etsi.org/ngsi-ld/createdAt)|(https://uri.etsi.org/ngsi-ld/modifiedAt)|(https://uri.etsi.org/ngsi-ld/observedAt)|(https://uri.etsi.org/ngsi-ld/observationSpace)|(https://uri.etsi.org/ngsi-ld/operationSpace)|(https://uri.etsi.org/ngsi-ld/attributes)|(https://uri.etsi.org/ngsi-ld/information)|(https://uri.etsi.org/ngsi-ld/instanceId)|(https://uri.etsi.org/ngsi-ld/coordinates)|(https://uri.etsi.org/ngsi-ld/idPattern)|(https://uri.etsi.org/ngsi-ld/entities)|(https://uri.etsi.org/ngsi-ld/geometry)|(https://uri.etsi.org/ngsi-ld/geoQ)|(https://uri.etsi.org/ngsi-ld/accept)|(https://uri.etsi.org/ngsi-ld/uri)|(https://uri.etsi.org/ngsi-ld/endpoint)|(https://uri.etsi.org/ngsi-ld/format)|(https://uri.etsi.org/ngsi-ld/notification)|(https://uri.etsi.org/ngsi-ld/q)|(https://uri.etsi.org/ngsi-ld/watchedAttributes)|(https://uri.etsi.org/ngsi-ld/name)|(https://uri.etsi.org/ngsi-ld/throttling)|(https://uri.etsi.org/ngsi-ld/timeInterval)|(https://uri.etsi.org/ngsi-ld/expires)|(https://uri.etsi.org/ngsi-ld/status)|(https://uri.etsi.org/ngsi-ld/description)|(https://uri.etsi.org/ngsi-ld/georel)|(https://uri.etsi.org/ngsi-ld/timestamp)|(https://uri.etsi.org/ngsi-ld/start)|(https://uri.etsi.org/ngsi-ld/end)|(https://uri.etsi.org/ngsi-ld/subscriptionId)|(https://uri.etsi.org/ngsi-ld/notifiedAt)|(https://uri.etsi.org/ngsi-ld/data)|(https://uri.etsi.org/ngsi-ld/internal)|(https://uri.etsi.org/ngsi-ld/lastNotification)|(https://uri.etsi.org/ngsi-ld/lastFailure
			// )|(https://uri.etsi.org/ngsi-ld/lastSuccess)|(https://uri.etsi.org/ngsi-ld/timesSent)|([\<\"\'\=\;\(\)\>\?\*])
			if (keyType == 1) {
				throw new ResponseException(ErrorType.BadRequestData,
						"Forbidden characters in JSON key. Forbidden Characters are "
								+ NGSIConstants.NGSI_LD_FORBIDDEN_KEY_CHARS);
			} else if (keyType == -1 || keyType == 5 || keyType == 9) {
				if (keyType == 9) {
					if (protectRegistrationLocationEntry(mapValue, mapEntry, usedContext)) {
						continue;
					}
				}
				if (mapValue instanceof Map) {
					hasAttributes = preFlightCheck((Map<String, Object>) mapValue, usedContext, false, calledEndpoint,
							true) || hasAttributes;
				} else if (mapValue instanceof List) {
					hasAttributes = preFlightCheck((List) mapValue, usedContext, false, calledEndpoint, true)
							|| hasAttributes;
				}
			} else if (keyType == 2) {
				// ID
				validateUri((String) mapValue);
				hasValue = true;
			} else if (keyType == 3) {
				// TYPE
				String type = null;
				if (mapValue instanceof List) {
					type = validateUri((String) ((List) mapValue).get(0));
				} else if (mapValue instanceof String) {
					type = validateUri((String) mapValue);
				}
				if (type == null) {
					continue;
				}
				switch (type) {
				case NGSIConstants.NGSI_LD_GEOPROPERTY:
					isGeoProperty = true;
					break;
				case NGSIConstants.NGSI_LD_PROPERTY:
					isProperty = true;
					break;
				case NGSIConstants.NGSI_LD_RELATIONSHIP:
					isRelationship = true;
					break;
				case NGSIConstants.NGSI_LD_DATE_TIME:
					isDatetime = true;
					break;
				default:
					break;
				}
			} else if (keyType == 6) {
				value = checkHasValue(mapValue);
				hasValue = true;
			} else if (keyType == 7) {
				checkHasObject(mapValue);
				hasObject = true;
			} else if (keyType == 8) {
				hasAtValue = true;
			}
		}
		if ((calledEndpoint == AppConstants.ENTITIES_URL_ID || calledEndpoint == AppConstants.HISTORY_URL_ID)
				&& (isProperty && !hasValue)) {
			throw new ResponseException(ErrorType.BadRequestData, "You can't have properties without a value");
		}
		if ((calledEndpoint == AppConstants.ENTITIES_URL_ID || calledEndpoint == AppConstants.HISTORY_URL_ID)
				&& (isRelationship && !hasObject)) {
			throw new ResponseException(ErrorType.BadRequestData, "You can't have relationships without an object");
		}
		if ((calledEndpoint == AppConstants.ENTITIES_URL_ID || calledEndpoint == AppConstants.HISTORY_URL_ID)
				&& (isDatetime && !hasAtValue)) {
			throw new ResponseException(ErrorType.BadRequestData, "You can't have an empty datetime entry");
		}

		if ((calledEndpoint == AppConstants.ENTITIES_URL_ID || calledEndpoint == AppConstants.HISTORY_URL_ID)
				&& (customKey && !((isProperty && hasValue) || (isRelationship && hasObject)
						|| (isDatetime && hasAtValue) || (isGeoProperty && hasValue)))) {
			throw new ResponseException(ErrorType.BadRequestData, "Unknown entry");
		}
		if (isGeoProperty) {
			protectGeoProp(objMap, value, usedContext);
		}
		return hasAttributes;
	}

	private void checkHasObject(Object mapValue) throws ResponseException {
		if (mapValue == null) {
			throw new ResponseException(ErrorType.BadRequestData);
		}
		if (mapValue instanceof List) {
			List tempList = (List) mapValue;
			if(tempList.size() != 1) {
				throw new ResponseException(ErrorType.BadRequestData, "Only one entry per relationship is allowed");
			}
		}
		
		
	}

	public Subscription expandSubscription(String body, List<Object> contextLinks) throws ResponseException {
		Subscription subscription = new Subscription();

		Map<Integer, List<Object>> expanded;
		try {
			expanded = expand((Map<String, Object>) JsonUtils.fromString(body), contextLinks);
		} catch (Exception e) {
			throw new ResponseException(ErrorType.BadRequestData, "Failed to parse document. JSON is invalid");
		}
		Map<String, Object> rawSub = (Map<String, Object>) expanded.get(1).get(0);
		Object value = null;
		boolean hasEntities = false;
		boolean hasWatchedAttributes = false;
		boolean hasNotificaition = false;

		int keyType;
		for (Entry<String, Object> mapEntry : rawSub.entrySet()) {
			String key = mapEntry.getKey();
			Object mapValue = mapEntry.getValue();
			keyType = checkKey(key, subscriptionParser);
			/*
			 * // { JSON_LD_ID, JSON_LD_TYPE, JSON_LD_CONTEXT, NGSI_LD_ENTITIES,
			 * NGSI_LD_ID_PATTERN, NGSI_LD_GEO_QUERY, NGSI_LD_NOTIFICATION,
			 * NGSI_LD_ATTRIBUTES, NGSI_LD_ENDPOINT, NGSI_LD_ACCEPT, NGSI_LD_URI,
			 * NGSI_LD_FORMAT, NGSI_LD_QUERY, NGSI_LD_WATCHED_ATTRIBUTES,
			 * NGSI_LD_TIMES_SEND, NGSI_LD_THROTTLING, NGSI_LD_TIME_INTERVAL,
			 * NGSI_LD_TIMESTAMP_END, NGSI_LD_TIMESTAMP_START }
			 */
			if (keyType == 1) {
				throw new ResponseException(ErrorType.BadRequestData,
						"Forbidden characters in JSON key. Forbidden Characters are "
								+ NGSIConstants.NGSI_LD_FORBIDDEN_KEY_CHARS);
			} else if (keyType == -1) {
				throw new ResponseException(ErrorType.BadRequestData, "Unkown entry for subscription");
			} else if (keyType == 2) {
				// ID
				try {
					subscription.setId(new URI(validateUri((String) mapValue)));
				} catch (URISyntaxException e) {
					// Left empty intentionally is already checked
				}
			} else if (keyType == 3) {
				// TYPE
				String type = null;
				if (mapValue instanceof List) {
					type = validateUri((String) ((List) mapValue).get(0));
				} else if (mapValue instanceof String) {
					type = validateUri((String) mapValue);
				}
				if (type == null || !type.equals(NGSIConstants.NGSI_LD_SUBSCRIPTION)) {
					throw new ResponseException(ErrorType.BadRequestData, "No type or type is not Subscription");
				}
				subscription.setType(type);
			} else if (keyType == 5) {
				// Entities
				List<EntityInfo> entities = new ArrayList<EntityInfo>();
				List<Map<String, Object>> list = (List<Map<String, Object>>) mapValue;
				boolean hasType;
				for (Map<String, Object> entry : list) {
					EntityInfo entityInfo = new EntityInfo();
					hasType = false;
					for (Entry<String, Object> entitiesEntry : entry.entrySet()) {
						switch (entitiesEntry.getKey()) {
						case NGSIConstants.JSON_LD_ID:
							try {
								entityInfo.setId(new URI(validateUri((String) entitiesEntry.getValue())));
							} catch (URISyntaxException e) {
								// Left empty intentionally is already checked
							}
							break;
						case NGSIConstants.JSON_LD_TYPE:
							hasType = true;
							entityInfo.setType(validateUri((String) ((List) entitiesEntry.getValue()).get(0)));
							break;
						case NGSIConstants.NGSI_LD_ID_PATTERN:
							entityInfo.setIdPattern(
									(String) ((Map<String, Object>) ((List) entitiesEntry.getValue()).get(0))
											.get(NGSIConstants.JSON_LD_VALUE));
							break;
						default:
							throw new ResponseException(ErrorType.BadRequestData, "Unknown entry for entities");
						}
					}
					if (!hasType) {
						throw new ResponseException(ErrorType.BadRequestData, "Entities entry needs type");
					}
					hasEntities = true;
					entities.add(entityInfo);
				}
				subscription.setEntities(entities);
			} else if (keyType == 7) {
				try {
					LDGeoQuery ldGeoQuery = getGeoQuery((Map<String, Object>) ((List) mapValue).get(0));
					subscription.setLdGeoQuery(ldGeoQuery);
				} catch (Exception e) {
					logger.error(e);
					throw new ResponseException(ErrorType.BadRequestData, "Failed to parse geoQ");
				}
				// geoQ

			} else if (keyType == 8) {
				// NGSI_LD_NOTIFICATION
				try {
					NotificationParam notification = getNotificationParam(
							(Map<String, Object>) ((List) mapValue).get(0));
					subscription.setNotification(notification);
					hasNotificaition = true;
				} catch (Exception e) {
					throw new ResponseException(ErrorType.BadRequestData, "Failed to parse notification parameter.\n" + e.getMessage());
				}
			} else if (keyType == 14) {
				// NGSI_LD_QUERY

				try {
					subscription.setLdQuery(
							(String) ((List<Map<String, Object>>) mapValue).get(0).get(NGSIConstants.JSON_LD_VALUE));
				} catch (Exception e) {
					throw new ResponseException(ErrorType.BadRequestData, "Failed to parse geoQ");
				}
			} else if (keyType == 15) {
				// NGSI_LD_WATCHED_ATTRIBUTES
				try {
					subscription.setAttributeNames(getAttribs((List<Map<String, Object>>) mapValue));
				} catch (Exception e) {
					throw new ResponseException(ErrorType.BadRequestData,
							"Failed to parse watched attributes " + mapValue);
				}
			} else if (keyType == 17) {
				// THROTTELING
				try {
					subscription.setThrottling(
							(Integer) ((List<Map<String, Object>>) mapValue).get(0).get(NGSIConstants.JSON_LD_VALUE));
				} catch (Exception e) {
					throw new ResponseException(ErrorType.BadRequestData, "Failed to parse throtteling");
				}
			} else if (keyType == 18) {
				// TIMEINTERVALL
				try {
					subscription.setTimeInterval(
							(Integer) ((List<Map<String, Object>>) mapValue).get(0).get(NGSIConstants.JSON_LD_VALUE));
				} catch (Exception e) {
					throw new ResponseException(ErrorType.BadRequestData, "Failed to parse timeinterval");
				}
			} else if (keyType == 19) {
				// EXPIRES
				try {
					subscription.setExpires(SerializationTools.date2Long(
							(String) ((List<Map<String, Object>>) mapValue).get(0).get(NGSIConstants.JSON_LD_VALUE)));
				} catch (Exception e) {
					throw new ResponseException(ErrorType.BadRequestData, "Failed to parse expires");
				}
			} else if (keyType == 20) {
				// STATUS
				try {
					subscription.setStatus(
							(String) ((List<Map<String, Object>>) mapValue).get(0).get(NGSIConstants.JSON_LD_VALUE));
				} catch (Exception e) {
					throw new ResponseException(ErrorType.BadRequestData, "Failed to parse status");
				}
			} else if (keyType == 21) {
				// DESCRIPTION
				try {
					subscription.setDescription(
							(String) ((List<Map<String, Object>>) mapValue).get(0).get(NGSIConstants.JSON_LD_VALUE));
				} catch (Exception e) {
					throw new ResponseException(ErrorType.BadRequestData, "Failed to parse status");
				}
			} else if (keyType == 22) {
				// isActive
				try {
					subscription.setActive(
							(Boolean) ((List<Map<String, Object>>) mapValue).get(0).get(NGSIConstants.JSON_LD_VALUE));
				} catch (Exception e) {
					throw new ResponseException(ErrorType.BadRequestData, "Failed to parse active state");
				}
			}

		}

		if (!hasEntities && !hasWatchedAttributes) {
			throw new ResponseException(ErrorType.BadRequestData, "You have to specify watched attributes or entities");
		}
		if (!hasNotificaition) {
			throw new ResponseException(ErrorType.BadRequestData, "You have to specify notification");
		}

		return subscription;
	}

	private NotificationParam getNotificationParam(Map<String, Object> map) throws Exception {
		// Default accept
		String accept = AppConstants.NGB_APPLICATION_JSONLD;
		Format format = Format.normalized;
		List<String> watchedAttribs = new ArrayList<String>();
		String mqttVersion= null;
		Integer qos = null;
		NotificationParam notifyParam = new NotificationParam();
		Map<String,String>notifierInfo = new HashMap<String,String>();
		for (Entry<String, Object> entry : map.entrySet()) {
			switch (entry.getKey()) {
			case NGSIConstants.NGSI_LD_ATTRIBUTES:
				watchedAttribs = getAttribs((List<Map<String, Object>>) entry.getValue());
				notifyParam.setAttributeNames(watchedAttribs);
				break;
			case NGSIConstants.NGSI_LD_ENDPOINT:
				EndPoint endPoint = new EndPoint();
				for (Entry<String, Object> endPointEntry : ((List<Map<String, Object>>) entry.getValue()).get(0)
						.entrySet()) {
					switch (endPointEntry.getKey()) {
					case NGSIConstants.NGSI_LD_ACCEPT:
						accept = ((List<Map<String, String>>) endPointEntry.getValue()).get(0)
								.get(NGSIConstants.JSON_LD_VALUE);
						break;
					case NGSIConstants.NGSI_LD_URI:
						URI endPointURI = validateSubEndpoint(((List<Map<String, String>>) endPointEntry.getValue())
								.get(0).get(NGSIConstants.JSON_LD_VALUE));
						endPoint.setUri(endPointURI);
						break;
                    
					case NGSIConstants.NGSI_LD_NOTIFIERINFO:

						for (Entry<String, Object> endPointNotifier : ((List<Map<String, Object>>) endPointEntry
								.getValue()).get(0).entrySet()) {
							switch (endPointNotifier.getKey()) {
							case NGSIConstants.NGSI_LD_MQTT_VERSION:
								mqttVersion = validateSubNotifierInfoMqttVersion(
										((List<Map<String, String>>) endPointNotifier.getValue()).get(0)
												.get(NGSIConstants.JSON_LD_VALUE));
								notifierInfo.put(NGSIConstants.MQTT_VERSION, mqttVersion);
								break;
							case NGSIConstants.NGSI_LD_MQTT_QOS:
								qos = validateSubNotifierInfoQos(
										((List<Map<String, Integer>>) endPointNotifier.getValue()).get(0)
												.get(NGSIConstants.JSON_LD_VALUE));
								notifierInfo.put(NGSIConstants.MQTT_QOS, String.valueOf(qos));
								break;
							default:
								notifierInfo.put(NGSIConstants.MQTT_VERSION, NGSIConstants.DEFAULT_MQTT_VERSION);
								notifierInfo.put(NGSIConstants.MQTT_QOS,String.valueOf(NGSIConstants.DEFAULT_MQTT_QOS));
							}
						}
						endPoint.setNotifierInfo(notifierInfo);
						break;
				   	
					default:
						throw new ResponseException(ErrorType.BadRequestData, "Unkown entry for endpoint");
					}
				}
				endPoint.setAccept(accept);
				//endPoint.setNotifierInfo(notifierInfo);
				notifyParam.setEndPoint(endPoint);
				break;
			case NGSIConstants.NGSI_LD_FORMAT:
				String formatString = (String) ((List<Map<String, Object>>) entry.getValue()).get(0)
						.get(NGSIConstants.JSON_LD_VALUE);
				if (formatString.equalsIgnoreCase("keyvalues")) {
					format = Format.keyValues;
				}
				break;
			default:
				throw new ResponseException(ErrorType.BadRequestData, "Unkown entry for notification");
			}

		}
		notifyParam.setFormat(format);
		return notifyParam;
	}

	private List<String> getAttribs(List<Map<String, Object>> entry) throws ResponseException {
		ArrayList<String> watchedAttribs = new ArrayList<String>();
		for (Map<String, Object> attribEntry : entry) {
			String temp = (String) attribEntry.get(NGSIConstants.JSON_LD_ID);
			if (temp.matches(NGSIConstants.NGSI_LD_FORBIDDEN_KEY_CHARS_REGEX)) {
				throw new ResponseException(ErrorType.BadRequestData, "Invalid character in attribute names");
			}
			watchedAttribs.add(temp);
		}
		if (watchedAttribs.isEmpty()) {
			throw new ResponseException(ErrorType.BadRequestData, "Empty watched attributes entry");
		}
		return watchedAttribs;
	}

	private URI validateSubEndpoint(String string) throws ResponseException {
		URI uri;
		try {
			uri = new URI(string);
			if (Arrays.binarySearch(NGSIConstants.VALID_SUB_ENDPOINT_SCHEMAS, uri.getScheme()) == -1) {
				throw new ResponseException(ErrorType.BadRequestData, "Unsupport endpoint scheme");
			}
		} catch (URISyntaxException e) {
			throw new ResponseException(ErrorType.BadRequestData, "Invalid endpoint");
		}
		return uri;
	}

	private LDGeoQuery getGeoQuery(Map<String, Object> map) throws Exception {
		LDGeoQuery geoQuery = new LDGeoQuery();
		List<Map<String, Object>> jsonCoordinates = (List<Map<String, Object>>) map
				.get(NGSIConstants.NGSI_LD_COORDINATES);
		ArrayList<Double> coordinates = new ArrayList<Double>();

		for (Map<String, Object> entry : jsonCoordinates) {
			Object tempValue = entry.get(NGSIConstants.JSON_LD_VALUE);
			if (tempValue instanceof Double) {
				coordinates.add((Double) tempValue);
			} else if (tempValue instanceof Integer) {
				coordinates.add(((Integer) tempValue).doubleValue());
			} else if (tempValue instanceof Long) {
				coordinates.add(((Long) tempValue).doubleValue());
			} else {
				throw new ResponseException(ErrorType.BadRequestData, "Failed to parse coordinates");
			}

		}
		geoQuery.setCoordinates(coordinates);
		String geometry = (String) ((Map<String, Object>) ((List) map.get(NGSIConstants.NGSI_LD_GEOMETRY)).get(0))
				.get(NGSIConstants.JSON_LD_VALUE);
		if (geometry.equalsIgnoreCase("point")) {
			geoQuery.setGeometry(Geometry.Point);
		} else if (geometry.equalsIgnoreCase("polygon")) {
			geoQuery.setGeometry(Geometry.Polygon);
		}
		String geoRelString = (String) ((Map<String, Object>) ((List) map.get(NGSIConstants.NGSI_LD_GEO_REL)).get(0))
				.get(NGSIConstants.JSON_LD_VALUE);
		String[] relSplit = geoRelString.split(";");
		GeoRelation geoRel = new GeoRelation();
		geoRel.setRelation(relSplit[0]);
		for (int i = 1; i < relSplit.length; i++) {
			String[] temp = relSplit[i].split("==");
			Object distance;
			try {
				distance = Integer.parseInt(temp[1]);
			} catch (NumberFormatException e) {
				distance = Double.parseDouble(temp[1]);
			}
			if (temp[0].equalsIgnoreCase("maxDistance")) {

				geoRel.setMaxDistance(distance);
			} else if (temp[0].equalsIgnoreCase("minDistance")) {
				geoRel.setMinDistance(distance);
			}
		}
		geoQuery.setGeoRelation(geoRel);
		return geoQuery;
	}

	private int checkKey(String key, Pattern p) {
		Matcher m = p.matcher(key);
		int result = 10000;
		while (m.find()) {
			for (int i = 1; i <= m.groupCount(); i++) {
				if (m.group(i) == null) {
					continue;
				}
				if (result > i) {
					result = i;
					break;
				}
			}
		}
		return result;
	}

	private Object checkHasValue(Object mapValue) throws ResponseException {
		if (mapValue == null) {
			throw new ResponseException(ErrorType.BadRequestData);
		}
		if (mapValue instanceof List) {
			List tempList = (List) mapValue;
			if (!tempList.isEmpty())
				return tempList.get(0);
		}
		return null;
	}

	private boolean protectRegistrationLocationEntry(Object mapValue, Entry<String, Object> mapEntry,
			List<Object> usedContext) throws JsonGenerationException, IOException {
		if (((List) mapValue).get(0) instanceof Map) {
			Map temp = (Map) ((List) mapValue).get(0);
			if (temp.get(NGSIConstants.JSON_LD_TYPE) != null) {
				if (!((List) temp.get(NGSIConstants.JSON_LD_TYPE)).get(0).equals(NGSIConstants.NGSI_LD_GEOPROPERTY)) {
					// we are in a location entry of registry as this is not a geo property
					mapEntry.setValue(getProperGeoJson(mapValue, usedContext));
					return true;
				}
			}
		}
		return false;
	}

	private String validateUri(String mapValue) throws ResponseException {
		try {
			if (!new URI(mapValue).isAbsolute()) {
				throw new ResponseException(ErrorType.BadRequestData, "id is not a URI");
			}
			return mapValue;
		} catch (URISyntaxException e) {
			throw new ResponseException(ErrorType.BadRequestData, "id is not a URI");
		}

	}

	private Object getProperGeoJson(Object value, List<Object> usedContext)
			throws JsonGenerationException, IOException {
		Map<String, Object> compactedFull = JsonLdProcessor.compact(value, usedContext, defaultOptions);
		compactedFull.remove(NGSIConstants.JSON_LD_CONTEXT);
		String geoType = (String) compactedFull.get(NGSIConstants.GEO_JSON_TYPE);
		List geoValues = (List) compactedFull.get(NGSIConstants.GEO_JSON_COORDINATES);
		switch (geoType) {
		case NGSIConstants.GEO_TYPE_POINT:
			// nothing to be done here point is ok like this
			break;
		case NGSIConstants.GEO_TYPE_LINESTRING:
			ArrayList<Object> containerList = new ArrayList<Object>();
			for (int i = 0; i < geoValues.size(); i += 2) {
				ArrayList<Object> container = new ArrayList<Object>();
				container.add(geoValues.get(i));
				container.add(geoValues.get(i + 1));
				containerList.add(container);
			}
			compactedFull.put(NGSIConstants.GEO_JSON_COORDINATES, containerList);
			break;

		case NGSIConstants.GEO_TYPE_POLYGON:
			ArrayList<Object> topLevelContainerList = new ArrayList<Object>();
			ArrayList<Object> polyContainerList = new ArrayList<Object>();
			for (int i = 0; i < geoValues.size(); i += 2) {
				ArrayList<Object> container = new ArrayList<Object>();
				container.add(geoValues.get(i));
				container.add(geoValues.get(i + 1));
				polyContainerList.add(container);
			}
			topLevelContainerList.add(polyContainerList);
			compactedFull.put(NGSIConstants.GEO_JSON_COORDINATES, topLevelContainerList);
			break;
		case NGSIConstants.GEO_TYPE_MULTI_POLYGON:
			ArrayList<Object> multitopLevelContainerList = new ArrayList<Object>();
			ArrayList<Object> multimidLevelContainerList = new ArrayList<Object>();
			ArrayList<Object> multipolyContainerList = new ArrayList<Object>();
			for (int i = 0; i < geoValues.size(); i += 2) {
				ArrayList<Object> container = new ArrayList<Object>();
				container.add(geoValues.get(i));
				container.add(geoValues.get(i + 1));
				multipolyContainerList.add(container);
			}
			multimidLevelContainerList.add(multipolyContainerList);
			multitopLevelContainerList.add(multimidLevelContainerList);
			compactedFull.put(NGSIConstants.GEO_JSON_COORDINATES, multitopLevelContainerList);
			break;
		default:
			break;
		}
		String proctedValue = JsonUtils.toString(compactedFull);
		// temp.replace("\"", "\\\"");
		ArrayList<Object> tempList = new ArrayList<Object>();
		Map<String, Object> tempMap = new HashMap<String, Object>();
		tempMap.put(NGSIConstants.JSON_LD_VALUE, proctedValue);
		tempList.add(tempMap);
		return tempList;
	}

	private void protectGeoProp(Map<String, Object> objMap, Object value, List<Object> usedContext)
			throws JsonGenerationException, IOException, ResponseException {
		Object potentialStringValue = ((Map) value).get(NGSIConstants.JSON_LD_VALUE);
		if (potentialStringValue != null) {
			return;
		}

		
		Map<String, Object> compactedFull = JsonLdProcessor.compact(value, usedContext, defaultOptions);
		compactedFull.remove(NGSIConstants.JSON_LD_CONTEXT);
		String geoType = (String) compactedFull.get(NGSIConstants.GEO_JSON_TYPE);
		//This is needed because one context could map from type which wouldn't work with the used context.
		//Used context is needed because something could map point 
		//This is not good but new geo type will come so this can go away at some time
		if(geoType == null) {
			compactedFull = JsonLdProcessor.compact(value, CORE_CONTEXT, defaultOptions);
			compactedFull.remove(NGSIConstants.JSON_LD_CONTEXT);
			geoType = (String) compactedFull.get(NGSIConstants.GEO_JSON_TYPE);
			
		}
		List geoValues = (List) compactedFull.get(NGSIConstants.GEO_JSON_COORDINATES);
		Object entry1, entry2;
		switch (geoType) {
		case NGSIConstants.GEO_TYPE_POINT:
			// nothing to be done here point is ok like this
			entry1 = geoValues.get(0);
			entry2 = geoValues.get(1);
			if((!(entry1 instanceof Double) && !(entry1 instanceof Integer)) || (!(entry2 instanceof Double) && !(entry2 instanceof Integer))) {
				throw new ResponseException(ErrorType.BadRequestData, "Provided coordinate entry is not a float value");
			}
			break;
		case NGSIConstants.GEO_TYPE_LINESTRING:
			ArrayList<Object> containerList = new ArrayList<Object>();
			for (int i = 0; i < geoValues.size(); i += 2) {
				ArrayList<Object> container = new ArrayList<Object>();
				entry1 = geoValues.get(i);
				entry2 = geoValues.get(i + 1);
				if((!(entry1 instanceof Double) && !(entry1 instanceof Integer)) || (!(entry2 instanceof Double) && !(entry2 instanceof Integer))) {
					throw new ResponseException(ErrorType.BadRequestData, "Provided coordinate entry is not a float value");
				}
				container.add(entry1);
				container.add(entry2);
				containerList.add(container);
			}
			compactedFull.put(NGSIConstants.GEO_JSON_COORDINATES, containerList);
			break;

		case NGSIConstants.GEO_TYPE_POLYGON:
			ArrayList<Object> topLevelContainerList = new ArrayList<Object>();
			ArrayList<Object> polyContainerList = new ArrayList<Object>();
			if(!geoValues.get(0).equals(geoValues.get(geoValues.size()-2)) || !geoValues.get(1).equals(geoValues.get(geoValues.size()-1))) {
				throw new ResponseException(ErrorType.BadRequestData, "Polygon does not close");
			}
			for (int i = 0; i < geoValues.size(); i += 2) {
				ArrayList<Object> container = new ArrayList<Object>();
				entry1 = geoValues.get(i);
				entry2 = geoValues.get(i + 1);
				if((!(entry1 instanceof Double) && !(entry1 instanceof Integer)) || (!(entry2 instanceof Double) && !(entry2 instanceof Integer))) {
					throw new ResponseException(ErrorType.BadRequestData, "Provided coordinate entry is not a float value");
				}
				container.add(entry1);
				container.add(entry2);
				polyContainerList.add(container);
			}
			topLevelContainerList.add(polyContainerList);
			compactedFull.put(NGSIConstants.GEO_JSON_COORDINATES, topLevelContainerList);
			break;
		case NGSIConstants.GEO_TYPE_MULTI_POLYGON:
			ArrayList<Object> multiTopLevelContainerList = new ArrayList<Object>();
			ArrayList<Object> multiMidLevelContainerList = new ArrayList<Object>();
			ArrayList<Object> multiPolyContainerList = new ArrayList<Object>();
			for (int i = 0; i < geoValues.size(); i += 2) {
				ArrayList<Object> container = new ArrayList<Object>();
				entry1 = geoValues.get(i);
				entry2 = geoValues.get(i + 1);
				if((!(entry1 instanceof Double) && !(entry1 instanceof Integer)) || (!(entry2 instanceof Double) && !(entry2 instanceof Integer))) {
					throw new ResponseException(ErrorType.BadRequestData, "Provided coordinate entry is not a float value");
				}
				container.add(entry1);
				container.add(entry2);multiPolyContainerList.add(container);
			}
			multiMidLevelContainerList.add(multiPolyContainerList);
			multiTopLevelContainerList.add(multiMidLevelContainerList);

			compactedFull.put(NGSIConstants.GEO_JSON_COORDINATES, multiTopLevelContainerList);
			break;

		default:
			break;
		}
		String proctedValue = JsonUtils.toString(compactedFull);
		// temp.replace("\"", "\\\"");
		ArrayList<Object> tempList = new ArrayList<Object>();
		Map<String, Object> tempMap = new HashMap<String, Object>();
		tempMap.put(NGSIConstants.JSON_LD_VALUE, proctedValue);
		tempList.add(tempMap);
		objMap.put(NGSIConstants.NGSI_LD_HAS_VALUE, tempList);
	}

	private void unprotectGeoProps(Object json) throws JsonParseException, IOException {
		if (json instanceof Map) {
			unprotectGeoProps((Map<String, Object>) json);
		} else if (json instanceof List) {
			unprotectGeoProps((List) json);
		}

	}

	private void unprotectGeoProps(Map<String, Object> objMap) throws JsonParseException, IOException {
		boolean typeFound = false;
		Object value = null;
		for (Entry<String, Object> mapEntry : objMap.entrySet()) {

			String key = mapEntry.getKey();
			Object mapValue = mapEntry.getValue();
			if (key.equals(NGSIConstants.JSON_LD_CONTEXT)) {
				continue;
			}
			if (key.equals(NGSIConstants.NGSI_LD_LOCATION_SHORT)) {
				if (mapValue instanceof String) {
					mapEntry.setValue(JsonUtils.fromString((String) mapValue));
					continue;
				}
			}
			if (key.equals(NGSIConstants.NGSI_LD_WATCHED_ATTRIBUTES_SHORT)
					|| key.equals(NGSIConstants.NGSI_LD_ATTRIBUTES_SHORT)
					|| key.equals(NGSIConstants.NGSI_LD_ENTITIES_SHORT)) {
				if (!(mapValue instanceof List)) {
					ArrayList<Object> temp = new ArrayList<Object>();
					temp.add(mapValue);
					mapEntry.setValue(temp);
				}
				continue;
			}

			if (NGSIConstants.QUERY_PARAMETER_TYPE.equals(key) && (mapValue instanceof String)) {

				if (NGSIConstants.NGSI_LD_GEOPROPERTY_SHORT.equals(mapValue)) {
					typeFound = true;
				}
				// if(tempObj instanceof Map) {
				// if(NGSIConstants.NGSI_LD_GEOPROPERTY.equals(((Map)tempObj).get(NGSIConstants.JSON_LD_VALUE))){
				// typeFound = true;
				// }
				// }

			} else if (NGSIConstants.VALUE.equals(key)) {
				value = mapValue;
			} else {
				if (mapValue instanceof Map) {
					unprotectGeoProps((Map<String, Object>) mapValue);
				} else if (mapValue instanceof List) {
					unprotectGeoProps((List) mapValue);
				}
			}
		}

		if (typeFound && value != null) {

			objMap.put(NGSIConstants.VALUE, JsonUtils.fromString((String) value));

		}

	}

	private void unprotectGeoProps(List<Object> objList) throws JsonParseException, IOException {
		for (Object entry : objList) {
			if (entry instanceof Map) {

				unprotectGeoProps((Map<String, Object>) entry);
			} else if (entry instanceof List) {
				unprotectGeoProps((List) entry);
			} else {
				// don't care for now i think
			}
		}

	}

	/**
	 * @param body expanded json ld version
	 * @return rdf representation of entity/entities
	 * @throws ResponseException
	 */
	public String getRDF(String body) throws ResponseException {
		try {
			RDFDataset rdf = (RDFDataset) JsonLdProcessor.toRDF(JsonUtils.fromString(body), defaultOptions);

			return RDFDatasetUtils.toNQuads(rdf);
		} catch (JsonParseException e) {
			e.printStackTrace();
			throw new ResponseException(ErrorType.InvalidRequest);
		} catch (IOException e) {
			e.printStackTrace();
			throw new ResponseException(ErrorType.InvalidRequest);
		}
	}

	public CompactedJson compact(String body, List<Object> contextLinks) throws ResponseException {
		try {
			Object json = JsonUtils.fromString(body);
			Map<String, Object> context = new HashMap<String, Object>();
			for (Object url : contextLinks) {
				context.putAll(getRemoteContext((String) url));
			}
			return compact(json, context, contextLinks);
		} catch (JsonParseException e) {
			e.printStackTrace();
			throw new ResponseException(ErrorType.InvalidRequest);
		} catch (IOException e) {
			e.printStackTrace();
			throw new ResponseException(ErrorType.InvalidRequest);
		}

	}

	public CompactedJson compact(String body) throws ResponseException {
		try {
			// This should anyway never happen as we only compact for output an there will
			// be no @context in that
			// Object json = JsonUtils.fromString(body);
			// List<Object> context;
			// context = (List<Object>) json.getOrDefault(NGSIConstants.JSON_LD_CONTEXT, new
			// ArrayList<Object>());
			// Map<String, Object> fullContext = getFullContext(context);
			// json.remove(NGSIConstants.JSON_LD_CONTEXT);
			return compact(JsonUtils.fromString(body), null, null);
		} catch (JsonParseException e) {
			e.printStackTrace();
			throw new ResponseException(ErrorType.InvalidRequest);
		} catch (IOException e) {
			e.printStackTrace();
			throw new ResponseException(ErrorType.InvalidRequest);
		}
	}

	private CompactedJson compact(Object json, Map<String, Object> context, List<Object> rawContext)
			throws ResponseException {
		// validateAndCleanContext(context);
		CompactedJson result = new CompactedJson();
		int hash = json.hashCode();
		if (context.containsKey(IS_FULL_VALID)) {
			result.setContextUrl((String) rawContext.get(0));
		} else {
			rawContext.add(CORE_CONTEXT_URL_STR);
			result.setContextUrl(generateAtContextServing(rawContext, hash));

		}
		context.remove(IS_FULL_VALID);
		try {
			cleanExpandedJson(json);
			Map<String, Object> tempResult = JsonLdProcessor.compact(json, rawContext, defaultOptions);
			unprotectGeoProps(tempResult);
//			unprotectLocationFromRegistry(tempResult);
			if (tempResult.containsKey("@graph")) {
				// we are in a multiresult set
				Object atContext = tempResult.get(NGSIConstants.JSON_LD_CONTEXT);
				List<Map<String, Object>> toCompact = (List<Map<String, Object>>) tempResult.get("@graph");
				result.setCompacted(JsonUtils.toPrettyString(toCompact));
				for (Map<String, Object> entry : toCompact) {
					entry.put(NGSIConstants.JSON_LD_CONTEXT, rawContext);
				}
				result.setCompactedWithContext(JsonUtils.toPrettyString(toCompact));
			} else {

				tempResult.put(NGSIConstants.JSON_LD_CONTEXT, rawContext);
				result.setCompactedWithContext(JsonUtils.toPrettyString(tempResult));
				tempResult.remove(NGSIConstants.JSON_LD_CONTEXT);
				result.setCompacted(JsonUtils.toPrettyString(tempResult));
			}

		} catch (IOException e) {
			e.printStackTrace();
			throw new ResponseException(ErrorType.InvalidRequest, e.getMessage());
		}
		return result;
	}

	private void cleanExpandedJson(Object json) {
		if(json instanceof List) {
			List tempList = (List)json;
			for(Object entry: tempList) {
				cleanExpandedJson(entry);
			}
		}else if(json instanceof Map) {
			Map tempMap = (Map)json;
			Iterator<Entry> it = tempMap.entrySet().iterator();
			while(it.hasNext()) {
				Entry next = it.next();
				Object key = next.getKey();
				Object value = next.getValue();
				if(NGSIConstants.NGSI_LD_DATA_SET_ID.equals(key) && NGSIConstants.DEFAULT_DATA_SET_ID.equals(((Map)((List)value).get(0)).get(NGSIConstants.JSON_LD_ID))) {
					it.remove();
					continue;
				}
				if(value instanceof Map || value instanceof List) {
					cleanExpandedJson(value);
				}
			}
		}
		 
	}

	private String generateAtContextServing(List<Object> rawContext, int hash) {
		ArrayList<Object> sorted = new ArrayList<Object>();
		if (rawContext != null && !rawContext.isEmpty()) {
			sorted.addAll(rawContext);
		}
		// sorted.add(DEFAULT_CONTEXT_URL);
		sorted.add(CORE_CONTEXT_URL);
		try {
			kafkaOps.pushToKafka(producerChannel.atContextWriteChannel(), (hash + "").getBytes(),
					DataSerializer.toJson(sorted).getBytes());
		} catch (ResponseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return AT_CONTEXT_BASE_URL + hash;
	}

	private Map<String, Object> getFullContext(Object context) throws ResponseException {
		Map<String, Object> result = new HashMap<String, Object>();
		if (context instanceof String) {
			// just another url
			String temp = (String) context;
			if (temp.equals(CORE_CONTEXT_URL_STR)) {
				result.put(IS_FULL_VALID, true);
			} else {
				// Don't download core again
				result.putAll(getRemoteContext(temp));
			}
		} else if (context instanceof List) {
			for (Object entry : (List) context) {
				if (entry instanceof String) {
					// just another url
					String temp = (String) entry;
					if (temp.equals(CORE_CONTEXT_URL_STR)) {
						result.put(IS_FULL_VALID, true);
					} else {
						try {
							result.putAll(getRemoteContext(entry.toString()));
						} catch (ResponseException e) {
							// this can happen as not all "remote" entries are really remote contexts
							// print error to show up in log and add "remote" as is to used context
							logger.warn(
									"Failed to get a remote context. This can happen as you can also just give a url. Check the error!"
											+ e.getMessage());
						}
					}
				} else if (entry instanceof Map) {
					result.putAll(((Map) entry));
				} else if (entry instanceof List) {
					result.putAll(getFullContext(entry));
				} else {
					// Everything else should be illegal for @context
					throw new ResponseException(ErrorType.BadRequestData, "Illegal state of @context");
				}
			}
		} else if (context instanceof Map) {
			// @context entries, straight key value pairs
			result.putAll(((Map) context));
		} else {
			// Everything else should be illegal for @context
			throw new ResponseException(ErrorType.BadRequestData, "Illegal state of @context");
		}
		return result;
	}

	private Map<String, Object> getRemoteContext(String url) throws ResponseException {
		try {

			String body = httpUtils.doGet(new URI(url));
			Map<String, Object> remoteContext = (Map<String, Object>) JsonUtils.fromString(body);
			Object temp = remoteContext.get(NGSIConstants.JSON_LD_CONTEXT);
			if (temp == null) {
				throw new ResponseException(ErrorType.BadRequestData, "Failed to get remote @context from " + url);
			}
			return getFullContext(temp);

		} catch (IOException | URISyntaxException e) {
			throw new ResponseException(ErrorType.BadRequestData, "Failed to get remote @context from " + url);
		}

	}

	private void validateAndCleanContext(Map<String, Object> contextToMerge) throws ResponseException {
		if (contextToMerge == null) {
			return;
		}
		Iterator<Entry<String, Object>> it = contextToMerge.entrySet().iterator();

		while (it.hasNext()) {
			Entry<String, Object> next = it.next();
			String key = next.getKey();
			Object value = next.getValue();
			if (BASE_CONTEXT.containsKey(key)) {
				if (!value.equals(BASE_CONTEXT.get(key))) {
					// Attemp to overwrite default context
					throw new ResponseException(ErrorType.BadRequestData,
							"Provided context entry " + key + "=" + value.toString() + " overrides base context");
				}
				it.remove();
				continue;
			}

		}
	}
	
	private String validateSubNotifierInfoMqttVersion(String string) throws ResponseException {
		try {
			if (!Arrays.asList(NGSIConstants.VALID_MQTT_VERSION).contains(string)) {
				throw new ResponseException(ErrorType.BadRequestData, "Unsupport Mqtt version");
			}
		} catch (Exception e) {
			throw new ResponseException(ErrorType.BadRequestData, "Unsupport Mqtt version");
		}
		return string;
	}

	private int validateSubNotifierInfoQos(Integer qos) throws ResponseException {
		try {
			if (!Arrays.asList(NGSIConstants.VALID_QOS).contains(qos)) {
				throw new ResponseException(ErrorType.BadRequestData, "Unsupport Qos");
			}
		} catch (Exception e) {
			throw new ResponseException(ErrorType.BadRequestData, "Unsupport Qos");
		}
		return qos;
	}

}
