package eu.neclab.ngsildbroker.commons.datatypes;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdConsts;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.terms.DataSetIdTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.GeoQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.LanguageQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.OmitTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.PickTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.QQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.ScopeQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TemporalQueryTerm;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.enums.Format;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.QueryParser;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;
import io.vertx.core.http.impl.headers.HeadersMultiMap;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:23
 */

public class Subscription implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = -327073906884724592L;
	@JsonIgnore
	static final JsonLdOptions opts = new JsonLdOptions(JsonLdOptions.JSON_LD_1_1);
	private String description;
	private Long expiresAt = Long.MAX_VALUE;
	private String id;
	private String subscriptionName;
	private NotificationParam notification;
	private String status = "active";
	private Integer throttling = 0;
	private Integer timeInterval = 0;
	private String type;
	private String jsonldContext;
	private List<URI> requestorList;
	private Boolean isActive = true;
	private Set<String> attributeNames;
	private List<EntityInfo> entities;
	private LanguageQueryTerm langQuery;
	private String ldQueryString;
	private String scopeQueryString;
	private String csfQueryString;
	private GeoQueryTerm ldGeoQuery;
	private TemporalQueryTerm ldTempQuery;
	private Set<String> notificationTrigger = Sets.newHashSet();
	@JsonIgnore
	private HeadersMultiMap otherHead = new HeadersMultiMap();
	@JsonIgnore
	private QQueryTerm ldQuery;
	@JsonIgnore
	private QQueryTerm csfQuery;
	@JsonIgnore
	private ScopeQueryTerm scopeQuery;
	private DataSetIdTerm datasetIdTerm;

	private boolean localOnly = false;

	public boolean isLocalOnly() {
		return localOnly;
	}

	public void setLocalOnly(boolean localOnly) {
		this.localOnly = localOnly;
	}

	public Subscription() {
	}

	// duplicate
	public Subscription(Subscription subscription) {
		this.description = subscription.description;
		if (this.expiresAt != null) {
			this.expiresAt = subscription.expiresAt.longValue();
		}
		this.id = subscription.id;
		this.subscriptionName = subscription.subscriptionName;
		this.notification = new NotificationParam(subscription.notification);
		this.status = subscription.status;
		this.throttling = subscription.throttling.intValue();
		this.timeInterval = subscription.timeInterval.intValue();
		this.type = subscription.type;
		if (requestorList != null) {
			this.requestorList = new ArrayList<URI>(subscription.requestorList);
		}
		this.isActive = subscription.isActive;
		if (attributeNames != null) {
			this.attributeNames = Sets.newHashSet(subscription.attributeNames);
		}
		if (subscription.entities != null) {
			this.entities = new ArrayList<EntityInfo>(subscription.entities);
		}
		this.ldQueryString = subscription.ldQueryString;
		this.csfQueryString = subscription.csfQueryString;
		this.ldGeoQuery = subscription.ldGeoQuery;
		this.ldTempQuery = subscription.ldTempQuery;
		this.ldQuery = subscription.ldQuery;
		this.csfQuery = subscription.csfQuery;
	}

	public String getJsonldContext() {
		return jsonldContext;
	}

	public void setJsonldContext(String jsonldContext) {
		this.jsonldContext = jsonldContext;
	}

	public static Subscription expandSubscription(Map<String, Object> body, Context context, boolean update)
			throws ResponseException {
		return expandSubscription(body, null, context, update);
	}

	@SuppressWarnings("unchecked")
	public static Subscription expandSubscription(Map<String, Object> body, String id, Context context, boolean update)
			throws ResponseException {
		Subscription subscription = new Subscription();
		subscription.setId(id);
		for (Entry<String, Object> mapEntry : body.entrySet()) {
			String key = mapEntry.getKey();
			Object mapValue = mapEntry.getValue();

			switch (key) {
			case NGSIConstants.NGSI_LD_JSONLD_CONTEXT:
				subscription.setJsonldContext(((List<Map<String, String>>) mapValue).get(0).get(JsonLdConsts.VALUE));
				break;
			case NGSIConstants.NGSI_LD_LOCALONLY:
				if (!(mapValue instanceof List<?> list
						&& ((Map<String, Object>) (list.get(0))).get(NGSIConstants.JSON_LD_VALUE) instanceof Boolean)) {
					throw new ResponseException(ErrorType.BadRequestData, "localOnly should be boolean");
				}
				subscription
						.setLocalOnly(((List<Map<String, Boolean>>) mapValue).get(0).get(NGSIConstants.JSON_LD_VALUE));
				break;
			case NGSIConstants.JSON_LD_ID:
				subscription.setId((String) mapValue);
				break;
			case NGSIConstants.JSON_LD_TYPE:
				if (mapValue instanceof String) {
					subscription.setType((String) mapValue);
				} else if (mapValue instanceof List) {
					subscription.setType(((List<String>) mapValue).get(0));
				}
				break;
			case NGSIConstants.NGSI_LD_NOTIFICATION_TRIGGER:
				if (mapValue instanceof List) {
					List<Map<String, String>> triggers = (List<Map<String, String>>) mapValue;
					Set<String> notificationTriggers = subscription.getNotificationTrigger();
					for (Map<String, String> trigger : triggers) {
						String triggerValue = trigger.get(NGSIConstants.JSON_LD_VALUE);
						if (!NGSIConstants.NGSI_LD_NOTIFICATION_TRIGGER_VALID_VALUES.contains(triggerValue)) {
							throw new ResponseException(ErrorType.BadRequestData,
									"Invalid value for notificationTrigger. Valid values are " + String.join(",",
											NGSIConstants.NGSI_LD_NOTIFICATION_TRIGGER_VALID_VALUES));
						}
						notificationTriggers.add(triggerValue);
					}
				}
				break;
			case NGSIConstants.NGSI_LD_ENTITIES:
				List<EntityInfo> entities = new ArrayList<EntityInfo>();
				List<Map<String, Object>> list1 = (List<Map<String, Object>>) mapValue;
				boolean hasType;
				for (Map<String, Object> entry : list1) {
					EntityInfo entityInfo = new EntityInfo();
					hasType = false;
					for (Entry<String, Object> entitiesEntry : entry.entrySet()) {
						switch (entitiesEntry.getKey()) {
						case NGSIConstants.JSON_LD_ID:
							entityInfo.setId(((String) entitiesEntry.getValue()).split(","));
							for (String tmpId : entityInfo.getId()) {
								HttpUtils.validateUri(tmpId);
							}
							break;
						case NGSIConstants.JSON_LD_TYPE:
							hasType = true;

							entityInfo.setTypeTerm(QueryParser
									.parseTypeQuery(((List<String>) entitiesEntry.getValue()).get(0), context));
							if (entityInfo.getTypeTerm().getAllTypes().contains(NGSIConstants.NGSI_LD_STAR)) {
								subscription.setLocalOnly(true);
							}
							break;
						case NGSIConstants.NGSI_LD_ID_PATTERN:
							entityInfo.setIdPattern((String) ((List<Map<String, Object>>) entitiesEntry.getValue())
									.get(0).get(NGSIConstants.JSON_LD_VALUE));
							break;
						default:
							throw new ResponseException(ErrorType.BadRequestData, "Unknown entry for entities");
						}
					}
					if (!hasType) {
						throw new ResponseException(ErrorType.BadRequestData, "Entities entry needs type");
					}
					entities.add(entityInfo);
				}
				subscription.setEntities(entities);
				break;
			case NGSIConstants.NGSI_LD_GEO_QUERY:
				try {
					GeoQueryTerm ldGeoQuery = getGeoQuery(((List<Map<String, Object>>) mapValue).get(0), context);
					subscription.setLdGeoQuery(ldGeoQuery);
				} catch (Exception e) {
					throw new ResponseException(ErrorType.BadRequestData, "Failed to parse geoQ");
				}
				break;
			case NGSIConstants.NGSI_LD_NOTIFICATION:
				try {
					NotificationParam notification = getNotificationParam(((List<Map<String, Object>>) mapValue).get(0),
							context);
					subscription.setNotification(notification);
				} catch (Exception e) {
					throw new ResponseException(ErrorType.BadRequestData,
							"Failed to parse notification parameter." + e.getMessage());
				}
				break;
			case NGSIConstants.NGSI_LD_QUERY:
				try {
					subscription.setLdQueryString(
							(String) ((List<Map<String, Object>>) mapValue).get(0).get(NGSIConstants.JSON_LD_VALUE),
							context);
				} catch (Exception e) {
					e.printStackTrace();
					throw new ResponseException(ErrorType.BadRequestData, "Failed to parse q");
				}
				break;
			case NGSIConstants.NGSI_LD_WATCHED_ATTRIBUTES:
				try {
					subscription.setAttributeNames(getAttribs((List<Map<String, Object>>) mapValue));
				} catch (Exception e) {
					throw new ResponseException(ErrorType.BadRequestData,
							"Failed to parse watched attributes " + mapValue);
				}
				break;
			case NGSIConstants.NGSI_LD_THROTTLING:
				try {
					subscription.setThrottling(
							(Integer) ((List<Map<String, Object>>) mapValue).get(0).get(NGSIConstants.JSON_LD_VALUE));
				} catch (Exception e) {
					throw new ResponseException(ErrorType.BadRequestData, "Failed to parse throtteling");
				}
				break;
			case NGSIConstants.NGSI_LD_TIME_INTERVAL:
				try {
					subscription.setTimeInterval(
							(Integer) ((List<Map<String, Object>>) mapValue).get(0).get(NGSIConstants.JSON_LD_VALUE));
				} catch (Exception e) {
					throw new ResponseException(ErrorType.BadRequestData, "Failed to parse timeinterval");
				}
				break;
			case NGSIConstants.NGSI_LD_EXPIRES:
				try {
					subscription.setExpiresAt(SerializationTools.date2Long(
							(String) ((List<Map<String, Object>>) mapValue).get(0).get(NGSIConstants.JSON_LD_VALUE)));
				} catch (Exception e) {
					throw new ResponseException(ErrorType.BadRequestData, "Failed to parse expiresAt");
				}
				break;
			case NGSIConstants.NGSI_LD_STATUS:
				try {
					subscription.setStatus(
							(String) ((List<Map<String, Object>>) mapValue).get(0).get(NGSIConstants.JSON_LD_VALUE));
				} catch (Exception e) {
					throw new ResponseException(ErrorType.BadRequestData, "Failed to parse status");
				}
				break;
			case NGSIConstants.NGSI_LD_DESCRIPTION:
				try {
					subscription.setDescription(
							(String) ((List<Map<String, Object>>) mapValue).get(0).get(NGSIConstants.JSON_LD_VALUE));
				} catch (Exception e) {
					throw new ResponseException(ErrorType.BadRequestData, "Failed to parse status");
				}
				break;
			case NGSIConstants.NGSI_LD_IS_ACTIVE:
				try {
					subscription.setActive(
							(Boolean) ((List<Map<String, Object>>) mapValue).get(0).get(NGSIConstants.JSON_LD_VALUE));
				} catch (Exception e) {
					throw new ResponseException(ErrorType.BadRequestData, "Failed to parse active state");
				}
				break;
			case NGSIConstants.NGSI_LD_SUBSCRIPTION_NAME:
				try {
					subscription.setSubscriptionName(
							(String) ((List<Map<String, Object>>) mapValue).get(0).get(NGSIConstants.JSON_LD_VALUE));
				} catch (Exception e) {
					throw new ResponseException(ErrorType.BadRequestData, "Failed to parse active state");
				}
				break;
			case NGSIConstants.NGSI_LD_CSF:
				try {
					subscription.setCsfQueryString(
							(String) ((List<Map<String, Object>>) mapValue).get(0).get(NGSIConstants.JSON_LD_VALUE),
							context);
				} catch (Exception e) {
					throw new ResponseException(ErrorType.BadRequestData, "Failed to parse csfQ");
				}

				break;
			case NGSIConstants.NGSI_LD_SCOPE_Q:
				try {
					subscription.setScopeQueryString(
							(String) ((List<Map<String, Object>>) mapValue).get(0).get(NGSIConstants.JSON_LD_VALUE));
				} catch (Exception e) {
					throw new ResponseException(ErrorType.BadRequestData, "Failed to parse scopeQ");
				}
				break;

			default:
				break;
			}
		}
		if (subscription.getNotificationTrigger().isEmpty()) {
			// adding default Triggers
			Set<String> notificationTriggers = subscription.getNotificationTrigger();
			notificationTriggers.add(NGSIConstants.NGSI_LD_NOTIFICATION_TRIGGER_ENTITY_CREATED);
			notificationTriggers.add(NGSIConstants.NGSI_LD_NOTIFICATION_TRIGGER_ENTITY_UPDATED);

		}
		validateSub(subscription, update);
		return subscription;
	}

	private static void validateSub(Subscription subscription, boolean update) throws ResponseException {
		NotificationParam notifi = subscription.getNotification();
		if ((notifi.getPick() != null && notifi.getOmit() != null)
				|| (notifi.getPick() != null && notifi.getAttrs() != null)
				|| (notifi.getAttrs() != null && notifi.getOmit() != null)) {
			throw new ResponseException(ErrorType.BadRequestData, "Omit, pick and attrs are mutually exclusive");
		}
		if (subscription.getThrottling() > 0 && subscription.getTimeInterval() > 0) {
			throw new ResponseException(ErrorType.BadRequestData, "throttling  and timeInterval cannot both be set");
		}
		if (subscription.getTimeInterval() > 0) {
			if (subscription.getAttributeNames() == null || subscription.getAttributeNames().isEmpty()) {
				return;
			}
			throw new ResponseException(ErrorType.BadRequestData,
					"watchedAttributes  and timeInterval cannot both be set");
		}
		if (update && subscription.getNotification() != null && subscription.getNotification().getEndPoint() == null) {
			throw new ResponseException(ErrorType.BadRequestData, "A subscription needs a notification endpoint entry");
		}
		if (!update
				&& (subscription.getNotification() == null || subscription.getNotification().getEndPoint() == null)) {
			throw new ResponseException(ErrorType.BadRequestData, "A subscription needs a notification endpoint entry");
		}

	}

	@SuppressWarnings("unchecked")
	public static NotificationParam getNotificationParam(Map<String, Object> map, Context context) throws Exception {
		// Default accept
		String accept = AppConstants.NGB_APPLICATION_JSONLD;
		Format format = Format.normalized;

		String mqttVersion = null;
		Integer qos = null;
		NotificationParam notifyParam = new NotificationParam();
		Map<String, String> notifierInfo = new HashMap<String, String>();
		for (Entry<String, Object> entry : map.entrySet()) {
			switch (entry.getKey()) {
			case NGSIConstants.NGSI_LD_ATTRIBUTES:
				List<Map<String, Object>> watchedAttribs = (List<Map<String, Object>>) entry.getValue();
				String attrs = getCompactedAttrsQueryString(watchedAttribs, context);
				notifyParam.setAttrs(QueryParser.parseAttrs(attrs, context));
				break;
			case NGSIConstants.NGSI_LD_PICK:
				List<Map<String, Object>> pickList = (List<Map<String, Object>>) entry.getValue();
				String pick = getCompactedQueryString(pickList, NGSIConstants.NGSI_LD_PICK, context);
				PickTerm pickTerm = new PickTerm();
				QueryParser.parseProjectionTerm(pickTerm, pick, context);
				notifyParam.setPick(pickTerm);
				break;
			case NGSIConstants.NGSI_LD_OMIT:
				List<Map<String, Object>> omitList = (List<Map<String, Object>>) entry.getValue();
				String omit = getCompactedQueryString(omitList, NGSIConstants.NGSI_LD_OMIT, context);
				OmitTerm omitTerm = OmitTerm.getNewRootInstance();
				QueryParser.parseProjectionTerm(omitTerm, omit, context);
				notifyParam.setOmit(omitTerm);
				break;
			case NGSIConstants.NGSI_LD_ENDPOINT:
				EndPoint endPoint = new EndPoint();
				for (Entry<String, Object> endPointEntry : ((List<Map<String, Object>>) entry.getValue()).get(0)
						.entrySet()) {
					switch (endPointEntry.getKey()) {
					case NGSIConstants.NGSI_LD_ACCEPT -> accept = ((List<Map<String, String>>) endPointEntry.getValue())
							.get(0).get(NGSIConstants.JSON_LD_VALUE);
					case NGSIConstants.NGSI_LD_URI -> {
						URI endPointURI = validateSubEndpoint(((List<Map<String, String>>) endPointEntry.getValue())
								.get(0).get(NGSIConstants.JSON_LD_VALUE));
						endPoint.setUri(endPointURI);
					}
					case NGSIConstants.NGSI_LD_NOTIFIERINFO -> {
						for (Entry<String, Object> endPointNotifier : ((List<Map<String, Object>>) endPointEntry
								.getValue()).get(0).entrySet()) {
							switch (endPointNotifier.getKey()) {
							case NGSIConstants.NGSI_LD_MQTT_VERSION -> {
								mqttVersion = validateSubNotifierInfoMqttVersion(
										((List<Map<String, String>>) endPointNotifier.getValue()).get(0)
												.get(NGSIConstants.JSON_LD_VALUE));
								notifierInfo.put(NGSIConstants.MQTT_VERSION, mqttVersion);
							}
							case NGSIConstants.NGSI_LD_MQTT_QOS -> {
								qos = validateSubNotifierInfoQos(
										((List<Map<String, Integer>>) endPointNotifier.getValue()).get(0)
												.get(NGSIConstants.JSON_LD_VALUE));
								notifierInfo.put(NGSIConstants.MQTT_QOS, String.valueOf(qos));
							}
							default -> {
								notifierInfo.put(NGSIConstants.MQTT_VERSION, NGSIConstants.DEFAULT_MQTT_VERSION);
								notifierInfo.put(NGSIConstants.MQTT_QOS,
										String.valueOf(NGSIConstants.DEFAULT_MQTT_QOS));
							}
							}
						}
						endPoint.setNotifierInfo(notifierInfo);
					}
					case NGSIConstants.NGSI_LD_RECEIVERINFO -> {
						ArrayListMultimap<String, String> receiverInfo = ArrayListMultimap.create();
						Map<String, Object> compacted = JsonLdProcessor
								.compactWithLoadedContext(endPointEntry.getValue(), null, context, opts, 999);
						List<Map<String, Object>> receiverInfos = (List<Map<String, Object>>) compacted
								.get(JsonLdConsts.GRAPH);
						if (receiverInfos == null) {
							receiverInfos = Lists.newArrayList();
							compacted.remove(NGSIConstants.JSON_LD_CONTEXT);
							receiverInfos.add(compacted);
						}

						for (Map<String, Object> headerEntry : receiverInfos) {
							if (headerEntry.containsKey(NGSIConstants.KEY)) {
								receiverInfo.put(headerEntry.get(NGSIConstants.KEY).toString(),
										headerEntry.get(NGSIConstants.VALUE).toString());
							} else if (headerEntry.containsKey(NGSIConstants.NGSI_LD_HAS_KEY)) {
								receiverInfo.put(headerEntry.get(NGSIConstants.NGSI_LD_HAS_KEY).toString(),
										headerEntry.get(NGSIConstants.NGSI_LD_HAS_VALUE).toString());
							} else {
								headerEntry.forEach((t, u) -> {
									receiverInfo.put(t, u.toString());
								});
							}
						}
						endPoint.setReceiverInfo(receiverInfo);
					}
					default -> throw new ResponseException(ErrorType.BadRequestData,
							"Unkown entry for endpoint: " + entry.getKey());
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
				if (formatString.equalsIgnoreCase("concise")) {
					format = Format.concise;
				}
				break;
			case NGSIConstants.NGSI_LD_SYS_ATTRS:
				notifyParam.setSysAttrs(
						((List<Map<String, Boolean>>) entry.getValue()).get(0).get(NGSIConstants.JSON_LD_VALUE));
				break;
			case NGSIConstants.NGSI_LD_SHOWCHANGES:
				notifyParam.setShowChanges(
						((List<Map<String, Boolean>>) entry.getValue()).get(0).get(NGSIConstants.JSON_LD_VALUE));
				break;
			case NGSIConstants.NGSI_LD_TIMES_SENT:
				notifyParam.setTimesSent(
						((List<Map<String, Integer>>) entry.getValue()).get(0).get(NGSIConstants.JSON_LD_VALUE));
				break;
			case NGSIConstants.NGSI_LD_LAST_FAILURE:
			case NGSIConstants.NGSI_LD_LAST_SUCCESS:
			case NGSIConstants.NGSI_LD_LAST_NOTIFICATION:
				break;
			default:
				throw new ResponseException(ErrorType.BadRequestData, "Unkown entry for notification");
			}
		}
		notifyParam.setFormat(format);
		return notifyParam;
	}

	private static String getCompactedAttrsQueryString(List<Map<String, Object>> watchedAttribs, Context context) {
		StringBuilder result = new StringBuilder();
		watchedAttribs.forEach(entry -> {
			result.append(context.compactIri((String) entry.get(NGSIConstants.JSON_LD_ID)));
			result.append(',');
		});
		result.setLength(result.length() - 1);
		return result.toString();
	}

	private static String getCompactedQueryString(List<Map<String, Object>> watchedAttribs, String attribName,
			Context context) {
		StringBuilder tmp = new StringBuilder();
		for (Map<String, Object> listEntry : watchedAttribs) {
			tmp.append(context.compactValue(attribName, listEntry));
			tmp.append(',');
		}
		tmp.setLength(tmp.length() - 1);
		return tmp.toString();
	}

	@SuppressWarnings("unchecked")
	public static GeoQueryTerm getGeoQuery(Map<String, Object> map, Context context) throws Exception {

		List<Map<String, Object>> jsonCoordinates = (List<Map<String, Object>>) map
				.get(NGSIConstants.NGSI_LD_COORDINATES);
		String georel = (String) ((List<Map<String, Object>>) map.get(NGSIConstants.NGSI_LD_GEO_REL)).get(0)
				.get(NGSIConstants.JSON_LD_VALUE);
		String coordinates = getCoordinates(jsonCoordinates);
		String geoproperty = null;
		String geometry = (String) ((List<Map<String, Object>>) map.get(NGSIConstants.NGSI_LD_GEOMETRY)).get(0)
				.get(NGSIConstants.JSON_LD_VALUE);
		Object geoProperty = map.get(NGSIConstants.NGSI_LD_GEOPROPERTY_GEOQ_ATTRIB);
		if (geoProperty != null) {
			geoproperty = ((List<Map<String, String>>) geoProperty).get(0).get(NGSIConstants.JSON_LD_VALUE);
		}
		return QueryParser.parseGeoQuery(georel, coordinates, geometry, geoproperty, context);
	}

	@SuppressWarnings("unchecked")
	public static String getCoordinates(List<Map<String, Object>> jsonCoordinates) {
		String result = "";
		boolean lon = true;
		for (Map<String, Object> entry : jsonCoordinates) {
			for (Entry<String, Object> entry1 : entry.entrySet()) {
				String key = entry1.getKey();
				Object value = entry1.getValue();
				if (key.equals(NGSIConstants.JSON_LD_VALUE)) {
					double myValue = 0;
					if (value instanceof Double) {
						myValue = (Double) value;
					} else if (value instanceof Integer) {
						myValue = ((Integer) value).doubleValue();
					} else if (value instanceof Long) {
						myValue = ((Long) value).doubleValue();
					}
					if (lon) {
						myValue = SerializationTools.getProperLon(myValue);
						result += myValue;
					} else {
						myValue = SerializationTools.getProperLat(myValue);
						result += myValue;
					}

					lon = !lon;
				} else if (key.equals(NGSIConstants.JSON_LD_LIST)) {
					result += "[" + getCoordinates((List<Map<String, Object>>) value) + "]";
				}
			}
			result += ",";
		}
		return result.substring(0, result.length() - 1);
	}

	public static Set<String> getAttribs(List<Map<String, Object>> entry) throws ResponseException {
		Set<String> watchedAttribs = Sets.newHashSet();
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

	private static String validateSubNotifierInfoMqttVersion(String string) throws ResponseException {
		try {
			if (!Arrays.asList(NGSIConstants.VALID_MQTT_VERSION).contains(string)) {
				throw new ResponseException(ErrorType.BadRequestData, "Unsupport Mqtt version");
			}
		} catch (Exception e) {
			throw new ResponseException(ErrorType.BadRequestData, "Unsupport Mqtt version");
		}
		return string;
	}

	private static int validateSubNotifierInfoQos(Integer qos) throws ResponseException {
		try {
			if (!Arrays.asList(NGSIConstants.VALID_QOS).contains(qos)) {
				throw new ResponseException(ErrorType.BadRequestData, "Unsupport Qos");
			}
		} catch (Exception e) {
			throw new ResponseException(ErrorType.BadRequestData, "Unsupport Qos");
		}
		return qos;
	}

	private static URI validateSubEndpoint(String string) throws ResponseException {
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

	public void update(Subscription subscription) {
		if (subscription.description != null) {
			this.description = subscription.description;
		}
		if (subscription.expiresAt != null) {
			this.expiresAt = subscription.expiresAt;
		}
		if (subscription.id != null) {
			this.id = subscription.id;
		}
		if (subscription.subscriptionName != null) {
			this.subscriptionName = subscription.subscriptionName;
		}
		if (subscription.notification != null) {
			this.notification.update(subscription.notification);
		}
		if (subscription.status != null) {
			this.status = subscription.status;
		}
		if (subscription.throttling != null) {
			this.throttling = subscription.throttling;
		}
		if (subscription.timeInterval != null) {
			this.timeInterval = subscription.timeInterval;
		}
		if (subscription.type != null) {
			this.type = subscription.type;
		}
		if (subscription.requestorList != null) {
			this.requestorList = subscription.requestorList;
		}
		if (subscription.isActive != null) {
			this.isActive = subscription.isActive;
		}
		if (subscription.attributeNames != null) {
			this.attributeNames = subscription.attributeNames;
		}
		if (subscription.entities != null) {
			this.entities = subscription.entities;
		}

		if (subscription.ldQueryString != null) {
			this.ldQueryString = subscription.ldQueryString;
		}
		if (subscription.csfQueryString != null) {
			this.csfQueryString = subscription.csfQueryString;
		}
		if (subscription.ldGeoQuery != null) {
			this.ldGeoQuery = subscription.ldGeoQuery;
		}
		if (subscription.ldTempQuery != null) {
			this.ldTempQuery = subscription.ldTempQuery;
		}
		if (subscription.ldQuery != null) {
			this.ldQuery = subscription.ldQuery;
		}
		if (subscription.csfQuery != null) {
			this.csfQuery = subscription.csfQuery;
		}
	}

	public Set<String> getAttributeNames() {
		return attributeNames;
	}

	public void setAttributeNames(Set<String> attributeNames) {
		this.attributeNames = attributeNames;
	}

	public List<EntityInfo> getEntities() {
		return entities;
	}

	public void setEntities(List<EntityInfo> entities) {
		this.entities = entities;
	}

	public GeoQueryTerm getLdGeoQuery() {
		return ldGeoQuery;
	}

	public void setLdGeoQuery(GeoQueryTerm ldGeoQuery) {
		this.ldGeoQuery = ldGeoQuery;
	}

	public TemporalQueryTerm getLdTempQuery() {
		return ldTempQuery;
	}

	public void setLdTempQuery(TemporalQueryTerm ldTempQuery) {
		this.ldTempQuery = ldTempQuery;
	}

	public List<URI> getRequestorList() {
		return requestorList;
	}

	public void setRequestorList(List<URI> requestorList) {
		this.requestorList = requestorList;
	}

	public void addEntityInfo(EntityInfo entity) {
		if (this.entities == null) {
			this.entities = new ArrayList<EntityInfo>();
		}
		this.entities.add(entity);
	}

	public void removeEntityInfo(EntityInfo entity) {
		if (this.entities == null) {
			return;
		}
		this.entities.remove(entity);
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public Long getExpiresAt() {
		return expiresAt;
	}

	public void setExpiresAt(Long expiresAt) {
		this.expiresAt = expiresAt;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getSubscriptionName() {
		return subscriptionName;
	}

	public void setSubscriptionName(String subscriptionName) {
		this.subscriptionName = subscriptionName;
	}

	public NotificationParam getNotification() {
		return notification;
	}

	public void setNotification(NotificationParam notification) {
		this.notification = notification;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public Integer getThrottling() {
		return throttling;
	}

	public void setThrottling(Integer throttling) {
		this.throttling = throttling;
	}

	public Integer getTimeInterval() {
		return timeInterval;
	}

	public void setTimeInterval(Integer timeInterval) {
		this.timeInterval = timeInterval;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public QQueryTerm getLdQuery() {
		return ldQuery;
	}

	public void finalize() throws Throwable {

	}

	public Boolean isActive() {
		return isActive;
	}

	public void setActive(Boolean isActive) {
		this.isActive = isActive;
		if (isActive != null) {
			if (isActive) {
				this.status = "active";
			} else {
				this.status = "paused";
			}
		}
	}

	public QQueryTerm getCsf() {
		return csfQuery;
	}

	public String getScopeQueryString() {
		return scopeQueryString;
	}

	public void setScopeQueryString(String scopeQueryString) throws ResponseException {
		this.scopeQueryString = scopeQueryString;
		if (scopeQueryString != null) {
			this.scopeQuery = QueryParser.parseScopeQuery(scopeQueryString);
		} else {
			this.scopeQuery = null;
		}
	}

	public String getLdQueryString() {
		return ldQueryString;
	}

	public void setLdQueryString(String ldQueryString, Context ldContext) throws ResponseException {
		if (ldQueryString.strip().isBlank()) {
			ldQueryString = null;
		}
		this.ldQueryString = ldQueryString;
		if (ldQueryString != null) {
			this.ldQuery = QueryParser.parseQuery(ldQueryString, ldContext);
		} else {
			this.ldQuery = null;
		}
	}

	public String getCsfQueryString() {
		return csfQueryString;
	}

	public void setCsfQueryString(String csfQueryString, Context ldContext) throws ResponseException {
		if (csfQueryString.strip().isBlank()) {
			csfQueryString = null;
		}
		this.csfQueryString = csfQueryString;
		if (csfQueryString != null) {
			this.csfQuery = QueryParser.parseQuery(csfQueryString, ldContext);
		} else {
			this.csfQuery = null;
		}
	}

	public Boolean getIsActive() {
		return isActive;
	}

	public QQueryTerm getCsfQuery() {
		return csfQuery;
	}

	public ScopeQueryTerm getScopeQuery() {
		return scopeQuery;
	}

	public Set<String> getNotificationTrigger() {
		return notificationTrigger;
	}

	public void setNotificationTrigger(Set<String> notificationTrigger) {
		this.notificationTrigger = notificationTrigger;
	}

	public HeadersMultiMap getOtherHead() {
		return otherHead;
	}

	public void setOtherHead(HeadersMultiMap otherHead) {
		this.otherHead = otherHead;
	}

	public void addOtherHead(String key, String value) {
		this.otherHead.add(key, value);
	}

	public DataSetIdTerm getDatasetIdTerm() {
		return datasetIdTerm;
	}

	public void setDatasetIdTerm(DataSetIdTerm datasetIdTerm) {
		this.datasetIdTerm = datasetIdTerm;
	}

	@Override
	public String toString() {
		return "Subscription [description=" + description + ", expiresAt=" + expiresAt + ", id=" + id
				+ ", subscriptionName=" + subscriptionName + ", notification=" + notification + ", status=" + status
				+ ", throttling=" + throttling + ", timeInterval=" + timeInterval + ", type=" + type
				+ ", requestorList=" + requestorList + ", isActive=" + isActive + ", attributeNames=" + attributeNames
				+ ", entities=" + entities + ", ldQueryString=" + ldQueryString + ", scopeQueryString="
				+ scopeQueryString + ", csfQueryString=" + csfQueryString + ", ldGeoQuery=" + ldGeoQuery
				+ ", ldTempQuery=" + ldTempQuery + ", ldQuery=" + ldQuery + ", csfQuery=" + csfQuery + ", scopeQuery="
				+ scopeQuery + "]";
	}

	public LanguageQueryTerm getLanguageQuery() {
		// TODO Auto-generated method stub
		return null;
	}



}
