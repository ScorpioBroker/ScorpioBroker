package eu.neclab.ngsildbroker.subscriptionmanager.controller;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.PostConstruct;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.github.jsonldjava.core.JsonLdConsts;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.EndPoint;
import eu.neclab.ngsildbroker.commons.datatypes.EntityInfo;
import eu.neclab.ngsildbroker.commons.datatypes.GeoRelation;
import eu.neclab.ngsildbroker.commons.datatypes.LDGeoQuery;
import eu.neclab.ngsildbroker.commons.datatypes.NotificationParam;
import eu.neclab.ngsildbroker.commons.datatypes.RestResponse;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.enums.Format;
import eu.neclab.ngsildbroker.commons.enums.Geometry;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.SubscriptionManager;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaOps;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;
import eu.neclab.ngsildbroker.commons.tools.ValidateURI;
import eu.neclab.ngsildbroker.commons.tools.Validator;

@RestController
@RequestMapping("/ngsi-ld/v1/subscriptions")
public class SubscriptionController {

	private final static Logger logger = LogManager.getLogger(SubscriptionController.class);
	private final static String MY_REQUEST_MAPPING = "/ngsi-ld/v1/subscriptions";
	private final static String MY_REQUEST_MAPPING_ALT = "/ngsi-ld/v1/subscriptions/";

	@Autowired
	SubscriptionManager manager;

	@Autowired
	@Qualifier("smops")
	KafkaOps kafkaOps;

	@Value("${atcontext.url}")
	String atContextServerUrl;

	@Autowired
	@Qualifier("smparamsResolver")
	ParamsResolver ldTools;

	@Value("${ngsild.corecontext:https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld}")
	String coreContext;

	@PostConstruct
	public void init() {
		JsonLdProcessor.init(coreContext);
	}

	ResponseException badRequest = new ResponseException(ErrorType.BadRequestData);

	ResponseEntity<byte[]> badRequestResponse = ResponseEntity.status(badRequest.getHttpStatus())
			.body(new RestResponse(badRequest).toJsonBytes());

	private HttpUtils httpUtils;
	private JsonLdOptions opts;
	private Pattern subscriptionParser;
	private Pattern attributeChecker;

	public SubscriptionController() {
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

	@RequestMapping(method = RequestMethod.POST)
	@ResponseBody
	public ResponseEntity<byte[]> subscribeRest(ServerHttpRequest request, @RequestBody String payload) {
		logger.trace("subscribeRest() :: started");
		Subscription subscription = null;

		try {
			// HttpUtils.doPreflightCheck(request, payload);
			List<Object> context = new ArrayList<Object>();
			context.addAll(HttpUtils.getAtContext(request));

			// System.out.println("RECEIVING SUBSCRIPTION: " + payload + " at " +
			// System.currentTimeMillis());
			subscription = expandSubscription(payload, request);
			Object bodyContext = ((Map<String, Object>) JsonUtils.fromString(payload)).get(JsonLdConsts.CONTEXT);
			if (bodyContext instanceof List) {
				context.addAll((List) bodyContext);
			} else {
				context.add(bodyContext);
			}

			SubscriptionRequest subRequest = new SubscriptionRequest(subscription, context,
					HttpUtils.getHeaders(request));
			URI subId = manager.subscribe(subRequest);

			logger.trace("subscribeRest() :: completed");
			return ResponseEntity.created(new URI(AppConstants.SUBSCRIPTIONS_URL + subId.toString())).build();
		} catch (ResponseException e) {
			logger.error("Exception ::", e);
			return ResponseEntity.status(e.getHttpStatus()).body(new RestResponse(e).toJsonBytes());
		} catch (URISyntaxException e) {
			logger.error("Exception ::", e);
			return ResponseEntity.status(HttpStatus.CONFLICT).body(subscription.getId().toString().getBytes());
		} catch (IOException e) {
			logger.error("Exception ::", e);
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(e.getMessage().getBytes());
		}
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

	public Subscription expandSubscription(String body, ServerHttpRequest request) throws ResponseException {
		Subscription subscription = new Subscription();

		Map<String, Object> rawSub = (Map<String, Object>) JsonLdProcessor.expand(HttpUtils.getAtContext(request), body,
				opts, AppConstants.SUBSCRIPTION_CREATE_PAYLOAD, HttpUtils.doPreflightCheck(request)).get(0);

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
					throw new ResponseException(ErrorType.BadRequestData,
							"Failed to parse notification parameter.\n" + e.getMessage());
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
					subscription.setExpiresAt(SerializationTools.date2Long(
							(String) ((List<Map<String, Object>>) mapValue).get(0).get(NGSIConstants.JSON_LD_VALUE)));
				} catch (Exception e) {
					throw new ResponseException(ErrorType.BadRequestData, "Failed to parse expiresAt");
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
			} else if (keyType == 25) {
				// Name
				try {
					subscription.setSubscriptionName(
							(String) ((List<Map<String, Object>>) mapValue).get(0).get(NGSIConstants.JSON_LD_VALUE));
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
		String mqttVersion = null;
		Integer qos = null;
		NotificationParam notifyParam = new NotificationParam();
		Map<String, String> notifierInfo = new HashMap<String, String>();
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
								notifierInfo.put(NGSIConstants.MQTT_QOS,
										String.valueOf(NGSIConstants.DEFAULT_MQTT_QOS));
							}
						}
						endPoint.setNotifierInfo(notifierInfo);
						break;

					default:
						throw new ResponseException(ErrorType.BadRequestData, "Unkown entry for endpoint");
					}
				}
				endPoint.setAccept(accept);
				// endPoint.setNotifierInfo(notifierInfo);
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

	@RequestMapping(method = RequestMethod.GET)
	public ResponseEntity<byte[]> getAllSubscriptions(ServerHttpRequest request,
			@RequestParam(required = false, name = "limit", defaultValue = "0") int limit) throws ResponseException {
		logger.trace("getAllSubscriptions() :: started");
		List<SubscriptionRequest> result = null;
		if (request.getPath().toString().equals(MY_REQUEST_MAPPING)
				|| request.getPath().toString().equals(MY_REQUEST_MAPPING_ALT)) {
			result = manager.getAllSubscriptions(limit, HttpUtils.getHeaders(request));
			logger.trace("getAllSubscriptions() :: completed");
			return HttpUtils.generateReply(request, DataSerializer.toJson(getSubscriptions(result)));
		} else {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST)
					.body(new RestResponse(ErrorType.BadRequestData, "Bad Request").toJsonBytes());
		}

	}

	private List<Subscription> getSubscriptions(List<SubscriptionRequest> subRequests) {
		ArrayList<Subscription> result = new ArrayList<Subscription>();
		for (SubscriptionRequest subRequest : subRequests) {
			result.add(subRequest.getSubscription());
		}
		return result;
	}

	@RequestMapping(method = RequestMethod.GET, value = "/{id}")
	public ResponseEntity<byte[]> getSubscriptions(ServerHttpRequest request,
			@PathVariable(name = NGSIConstants.QUERY_PARAMETER_ID, required = true) String id,
			@RequestParam(required = false, name = "limit", defaultValue = "0") int limit) {
		try {
			ValidateURI.validateUri(id);
			logger.trace("call getSubscriptions() ::");
			return httpUtils.generateReply(request, DataSerializer
					.toJson(manager.getSubscription(id, HttpUtils.getHeaders(request)).getSubscription()));

		} catch (ResponseException e) {
			logger.error("Exception ::", e);
			return ResponseEntity.status(e.getHttpStatus()).body(new RestResponse(e).toJsonBytes());
		}

	}

	@RequestMapping(method = RequestMethod.DELETE, value = "/{" + NGSIConstants.QUERY_PARAMETER_ID + "}")
	public ResponseEntity<byte[]> deleteSubscription(ServerHttpRequest request,
			@PathVariable(name = NGSIConstants.QUERY_PARAMETER_ID, required = true) URI id) {
		try {
			logger.trace("call deleteSubscription() ::");
			ValidateURI.validateUriInSubs(id);
			manager.unsubscribe(id, HttpUtils.getHeaders(request));
		} catch (ResponseException e) {
			logger.error("Exception ::", e);
			return ResponseEntity.status(e.getHttpStatus()).body(new RestResponse(e).toJsonBytes());
		}
		return ResponseEntity.noContent().build();
	}

	@RequestMapping(method = RequestMethod.DELETE)
	public ResponseEntity<byte[]> deleteSubscriptionEmptyId(ServerHttpRequest request) {
		return ResponseEntity.status(HttpStatus.BAD_REQUEST)
				.body(new RestResponse(ErrorType.BadRequestData, "Bad Request").toJsonBytes());
	}

	@RequestMapping(method = RequestMethod.PATCH, value = "/{" + NGSIConstants.QUERY_PARAMETER_ID + "}")
	public ResponseEntity<byte[]> updateSubscription(ServerHttpRequest request,
			@PathVariable(name = NGSIConstants.QUERY_PARAMETER_ID, required = true) URI id,
			@RequestBody String payload) {
		logger.trace("call updateSubscription() ::");

		try {

			ValidateURI.validateUriInSubs(id);
			Validator.subscriptionValidation(payload);
			List<Object> context = new ArrayList<Object>();
			context.addAll(HttpUtils.getAtContext(request));
			String resolved = JsonUtils
					.toString(JsonLdProcessor.expand(HttpUtils.getAtContext(request), JsonUtils.fromString(payload),
							opts, AppConstants.SUBSCRIPTION_UPDATE_PAYLOAD, HttpUtils.doPreflightCheck(request)));
			Subscription subscription = DataSerializer.getSubscription(resolved);
			if (subscription.getId() == null) {
				subscription.setId(id);
			}
			SubscriptionRequest subscriptionRequest = new SubscriptionRequest(subscription, context,
					HttpUtils.getHeaders(request));

			// expandSubscriptionAttributes(subscription, context);
			if (resolved == null || subscription == null || !id.equals(subscription.getId())) {
				return badRequestResponse;
			}
			manager.updateSubscription(subscriptionRequest);
		} catch (ResponseException e) {
			logger.error("Exception ::", e);
			return ResponseEntity.status(e.getHttpStatus()).body(new RestResponse(e).toJsonBytes());
		} catch (IOException e) {
			logger.error("Exception ::", e);
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(e.getMessage().getBytes());
		}
		return ResponseEntity.noContent().build();
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

	@RequestMapping(method = RequestMethod.PATCH)
	public ResponseEntity<byte[]> patchubscriptionEmptyId(ServerHttpRequest request) {
		return ResponseEntity.status(HttpStatus.BAD_REQUEST)
				.body(new RestResponse(ErrorType.BadRequestData, "Bad Request").toJsonBytes());
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
	// private void expandSubscriptionAttributes(Subscription subscription,
	// List<Object> context)
	// throws ResponseException {
	// for (EntityInfo info : subscription.getEntities()) {
	// if (info.getType() != null && !info.getType().trim().equals("")) {
	// info.setType(ldTools.expandAttribute(info.getType(), context));
	// }
	// }
	// if (subscription.getAttributeNames() != null) {
	// ArrayList<String> newAttribNames = new ArrayList<String>();
	// for (String attrib : subscription.getAttributeNames()) {
	// newAttribNames.add(ldTools.expandAttribute(attrib, context));
	// }
	// subscription.setAttributeNames(newAttribNames);
	// }
	// if (subscription.getNotification().getAttributeNames() != null) {
	// ArrayList<String> newAttribNames = new ArrayList<String>();
	// for (String attrib : subscription.getNotification().getAttributeNames()) {
	// newAttribNames.add(ldTools.expandAttribute(attrib, context));
	// }
	// subscription.getNotification().setAttributeNames(newAttribNames);
	//
	// }
	//
	// }

}
