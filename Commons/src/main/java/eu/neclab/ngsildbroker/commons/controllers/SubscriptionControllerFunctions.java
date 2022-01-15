package eu.neclab.ngsildbroker.commons.controllers;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdConsts;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.EndPoint;
import eu.neclab.ngsildbroker.commons.datatypes.EntityInfo;
import eu.neclab.ngsildbroker.commons.datatypes.GeoRelation;
import eu.neclab.ngsildbroker.commons.datatypes.LDGeoQuery;
import eu.neclab.ngsildbroker.commons.datatypes.NotificationParam;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.enums.Format;
import eu.neclab.ngsildbroker.commons.enums.Geometry;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.SubscriptionCRUDService;
import eu.neclab.ngsildbroker.commons.ngsiqueries.QueryParser;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;

public interface SubscriptionControllerFunctions {
	static final JsonLdOptions opts = new JsonLdOptions(JsonLdOptions.JSON_LD_1_1);

	@SuppressWarnings("unchecked")
	public static ResponseEntity<String> subscribeRest(SubscriptionCRUDService subscriptionService,
			HttpServletRequest request, String payload, String baseUrl, Logger logger) {
		logger.trace("subscribeRest() :: started");
		Subscription subscription = null;

		try {
			List<Object> linkHeaders = HttpUtils.getAtContext(request);
			List<Object> context = new ArrayList<Object>();
			context.addAll(linkHeaders);
			boolean atContextAllowed = HttpUtils.doPreflightCheck(request, linkHeaders);
			Map<String, Object> body = ((Map<String, Object>) JsonUtils.fromString(payload));
			Object bodyContext = body.get(JsonLdConsts.CONTEXT);
			body = (Map<String, Object>) JsonLdProcessor
					.expand(linkHeaders, body, opts, AppConstants.SUBSCRIPTION_CREATE_PAYLOAD, atContextAllowed).get(0);

			if (bodyContext instanceof List) {
				context.addAll((List<Object>) bodyContext);
			} else {
				context.add(bodyContext);
			}
			subscription = expandSubscription(body, request,
					JsonLdProcessor.getCoreContextClone().parse(context, true));
			SubscriptionRequest subRequest = new SubscriptionRequest(subscription, context,
					HttpUtils.getHeaders(request), AppConstants.CREATE_REQUEST);
			String subId = subscriptionService.subscribe(subRequest);

			logger.trace("subscribeRest() :: completed");
			return ResponseEntity.created(new URI(baseUrl + subId)).build();
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
	}

	@SuppressWarnings("unchecked")
	private static Subscription expandSubscription(Map<String, Object> body, HttpServletRequest request,
			Context context) throws ResponseException {
		Subscription subscription = new Subscription();

		for (Entry<String, Object> mapEntry : body.entrySet()) {
			String key = mapEntry.getKey();
			Object mapValue = mapEntry.getValue();

			switch (key) {
			case NGSIConstants.JSON_LD_ID:
				subscription.setId((String) mapValue);
				break;
			case NGSIConstants.JSON_LD_TYPE:
				subscription.setType(((List<String>) mapValue).get(0));
				break;
			case NGSIConstants.NGSI_LD_ENTITIES:
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
								entityInfo.setId(new URI((String) entitiesEntry.getValue()));
							} catch (URISyntaxException e) {
								// Left empty intentionally is already checked
							}
							break;
						case NGSIConstants.JSON_LD_TYPE:
							hasType = true;
							entityInfo.setType(((List<String>) entitiesEntry.getValue()).get(0));
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
					LDGeoQuery ldGeoQuery = getGeoQuery(((List<Map<String, Object>>) mapValue).get(0), context);
					subscription.setLdGeoQuery(ldGeoQuery);
				} catch (Exception e) {
					throw new ResponseException(ErrorType.BadRequestData, "Failed to parse geoQ");
				}
				break;
			case NGSIConstants.NGSI_LD_NOTIFICATION:
				try {
					NotificationParam notification = getNotificationParam(
							((List<Map<String, Object>>) mapValue).get(0));
					subscription.setNotification(notification);
				} catch (Exception e) {
					throw new ResponseException(ErrorType.BadRequestData,
							"Failed to parse notification parameter.\n" + e.getMessage());
				}
				break;
			case NGSIConstants.NGSI_LD_QUERY:
				try {

					subscription.setLdQuery(QueryParser.parseQuery(
							(String) ((List<Map<String, Object>>) mapValue).get(0).get(NGSIConstants.JSON_LD_VALUE),
							context).toSql());
				} catch (Exception e) {
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
					subscription.setCsf(QueryParser.parseQuery(
							(String) ((List<Map<String, Object>>) mapValue).get(0).get(NGSIConstants.JSON_LD_VALUE),
							context));
				} catch (Exception e) {
					throw new ResponseException(ErrorType.BadRequestData, "Failed to parse geoQ");
				}

				break;
			default:
				break;
			}
		}
		return subscription;
	}

	@SuppressWarnings("unchecked")
	private static NotificationParam getNotificationParam(Map<String, Object> map) throws Exception {
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

	public static ResponseEntity<String> getAllSubscriptions(SubscriptionCRUDService subscriptionService,
			HttpServletRequest request, int limit, int offset, boolean count, Logger logger) {
		logger.trace("getAllSubscriptions() :: started");
		if (limit < 0 || offset < 0) {
			return HttpUtils.handleControllerExceptions(
					new ResponseException(ErrorType.BadRequestData, "offset and limit can not smaller than 0"));
		}
		List<SubscriptionRequest> result = null;
		result = subscriptionService.getAllSubscriptions(HttpUtils.getHeaders(request));
		int toIndex = offset + limit;
		ArrayList<Object> additionalLinks = new ArrayList<Object>();
		if (limit == 0 || toIndex > result.size() - 1) {
			toIndex = result.size();
			if (toIndex < 0) {
				toIndex = 0;
			}

		} else {
			additionalLinks.add(HttpUtils.generateFollowUpLinkHeader(request, toIndex, limit, null, "next"));
		}

		if (offset > 0) {
			int newOffSet = offset - limit;
			if (newOffSet < 0) {
				newOffSet = 0;
			}
			additionalLinks.add(HttpUtils.generateFollowUpLinkHeader(request, newOffSet, limit, null, "prev"));
		}

		ArrayListMultimap<String, String> additionalHeaders = ArrayListMultimap.create();
		if (count == true) {
			additionalHeaders.put(NGSIConstants.COUNT_HEADER_RESULT, String.valueOf(result.size()));
		}
		if (!additionalLinks.isEmpty()) {
			for (Object entry : additionalLinks) {
				additionalHeaders.put(HttpHeaders.LINK, (String) entry);
			}
		}
		List<SubscriptionRequest> realResult = result.subList(offset, toIndex);
		logger.trace("getAllSubscriptions() :: completed");
		try {
			return HttpUtils.generateReply(request, DataSerializer.toJson(getSubscriptions(realResult)),
					additionalHeaders, AppConstants.SUBSCRIPTION_ENDPOINT);
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}

	}

	private static List<Subscription> getSubscriptions(List<SubscriptionRequest> subRequests) {
		ArrayList<Subscription> result = new ArrayList<Subscription>();
		for (SubscriptionRequest subRequest : subRequests) {
			result.add(subRequest.getSubscription());
		}
		return result;
	}

	public static ResponseEntity<String> getSubscriptionById(SubscriptionCRUDService subscriptionService,
			HttpServletRequest request, String id, int limit, Logger logger) {
		try {
			HttpUtils.validateUri(id);
			logger.trace("call getSubscriptions() ::");
			ArrayListMultimap<String, String> headers = HttpUtils.getHeaders(request);
			return HttpUtils.generateReply(request,
					DataSerializer.toJson(subscriptionService.getSubscription(id, headers).getSubscription()),
					AppConstants.SUBSCRIPTION_ENDPOINT);

		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
	}

	public static ResponseEntity<String> deleteSubscription(SubscriptionCRUDService subscriptionService,
			HttpServletRequest request, String id, Logger logger) {
		try {
			logger.trace("call deleteSubscription() ::");
			HttpUtils.validateUri(id);
			subscriptionService.unsubscribe(id, HttpUtils.getHeaders(request));
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
		return ResponseEntity.noContent().build();
	}

	@SuppressWarnings("unchecked")
	public static ResponseEntity<String> updateSubscription(SubscriptionCRUDService subscriptionService,
			HttpServletRequest request, String id, String payload, Logger logger) {
		logger.trace("call updateSubscription() ::");

		try {

			HttpUtils.validateUri(id);
			List<Object> linkHeaders = HttpUtils.getAtContext(request);
			boolean atContextAllowed = HttpUtils.doPreflightCheck(request, linkHeaders);
			List<Object> context = new ArrayList<Object>();
			context.addAll(linkHeaders);
			Map<String, Object> body = ((Map<String, Object>) JsonUtils.fromString(payload));
			Object bodyContext = body.get(JsonLdConsts.CONTEXT);
			body = (Map<String, Object>) JsonLdProcessor
					.expand(linkHeaders, body, opts, AppConstants.SUBSCRIPTION_UPDATE_PAYLOAD, atContextAllowed).get(0);

			if (bodyContext instanceof List) {
				context.addAll((List<Object>) bodyContext);
			} else {
				context.add(bodyContext);
			}
			Subscription subscription = expandSubscription(body, request,
					JsonLdProcessor.getCoreContextClone().parse(context, true));
			if (subscription.getId() == null) {
				subscription.setId(id);
			}
			SubscriptionRequest subscriptionRequest = new SubscriptionRequest(subscription, linkHeaders,
					HttpUtils.getHeaders(request), AppConstants.UPDATE_REQUEST);

			// expandSubscriptionAttributes(subscription, context);
			if (body == null || subscription == null || !id.equals(subscription.getId())) {
				return HttpUtils.handleControllerExceptions(
						new ResponseException(ErrorType.BadRequestData, "empty subscription body"));
			}
			subscriptionService.updateSubscription(subscriptionRequest);
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
		return ResponseEntity.noContent().build();
	}

	@SuppressWarnings("unchecked")
	private static LDGeoQuery getGeoQuery(Map<String, Object> map, Context context) throws Exception {
		LDGeoQuery geoQuery = new LDGeoQuery();
		Object geoProperty = map.get(NGSIConstants.NGSI_LD_GEOPROPERTY);
		if (geoProperty != null) {
			geoQuery.setGeoProperty(
					context.expandIri(((List<Map<String, String>>) geoProperty).get(0).get(NGSIConstants.JSON_LD_VALUE),
							false, true, null, null));
		}
		List<Map<String, Object>> jsonCoordinates = (List<Map<String, Object>>) map
				.get(NGSIConstants.NGSI_LD_COORDINATES);

		geoQuery.setCoordinates(getCoordinates(jsonCoordinates));
		String geometry = (String) ((List<Map<String, Object>>) map.get(NGSIConstants.NGSI_LD_GEOMETRY)).get(0)
				.get(NGSIConstants.JSON_LD_VALUE);
		if (geometry.equalsIgnoreCase("point")) {
			geoQuery.setGeometry(Geometry.Point);
		} else if (geometry.equalsIgnoreCase("polygon")) {
			geoQuery.setGeometry(Geometry.Polygon);
		} else if (geometry.equalsIgnoreCase("linestring")) {
			geoQuery.setGeometry(Geometry.LineString);
		}
		String geoRelString = (String) ((List<Map<String, Object>>) map.get(NGSIConstants.NGSI_LD_GEO_REL)).get(0)
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

	@SuppressWarnings("unchecked")
	public static ArrayList<Double> getCoordinates(List<Map<String, Object>> jsonCoordinates) {
		ArrayList<Double> result = new ArrayList<Double>();

		for (Map<String, Object> entry : jsonCoordinates) {
			for (Entry<String, Object> entry1 : entry.entrySet()) {
				String key = entry1.getKey();
				Object value = entry1.getValue();
				if (key.equals(NGSIConstants.JSON_LD_VALUE)) {
					if (value instanceof Double) {
						result.add((Double) value);
					} else if (value instanceof Integer) {
						result.add(((Integer) value).doubleValue());
					} else if (value instanceof Long) {
						result.add(((Long) value).doubleValue());
					}
				} else if (key.equals(NGSIConstants.JSON_LD_LIST)) {
					result.addAll(getCoordinates((List<Map<String, Object>>) value));
				}
			}
		}
		return result;
	}

	private static List<String> getAttribs(List<Map<String, Object>> entry) throws ResponseException {
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

}
