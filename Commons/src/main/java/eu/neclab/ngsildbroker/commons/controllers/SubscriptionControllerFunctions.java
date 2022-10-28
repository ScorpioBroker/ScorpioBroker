package eu.neclab.ngsildbroker.commons.controllers;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.util.MultiValueMap;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdConsts;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.GeoRelation;
import eu.neclab.ngsildbroker.commons.datatypes.LDGeoQuery;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.enums.Geometry;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.SubscriptionCRUDService;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
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
			if (bodyContext != null) {
				if (bodyContext instanceof List) {
					context.addAll((List<Object>) bodyContext);
				} else {
					context.add(bodyContext);
				}
			}
			subscription = Subscription.expandSubscription(body,
					JsonLdProcessor.getCoreContextClone().parse(context, true), false);
			if (subscription.isActive() == null) {
				subscription.setActive(true);
			}
			SubscriptionRequest subRequest = new SubscriptionRequest(subscription, context,
					HttpUtils.getHeaders(request), AppConstants.CREATE_REQUEST);
			String subId = subscriptionService.subscribe(subRequest);

			logger.trace("subscribeRest() :: completed");
			return ResponseEntity.created(new URI(baseUrl + subId)).build();
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
	}

	public static ResponseEntity<String> getAllSubscriptions(SubscriptionCRUDService subscriptionService,
			HttpServletRequest request, int defaultLimit, int maxLimit, Logger logger) {
		try {
			logger.trace("getAllSubscriptions() :: started");
			MultiValueMap<String, String> params = HttpUtils.getQueryParamMap(request);
			QueryParams qp = ParamsResolver.getQueryParamsFromUriQuery(params, JsonLdProcessor.getCoreContextClone(),
					false, false, defaultLimit, maxLimit);
			int limit = qp.getLimit();
			int offset = qp.getOffSet();
			if (limit > maxLimit) {
				return HttpUtils.handleControllerExceptions(new ResponseException(ErrorType.BadRequestData,
						"provided limit exceeds the max limit of " + maxLimit));
			}
			if (limit == 0) {
				limit = defaultLimit;
			}
			boolean count = qp.getCountResult();
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

			return HttpUtils.generateReply(request, JsonUtils.toPrettyString(getSubscriptions(realResult)),
					additionalHeaders, null, true, AppConstants.SUBSCRIPTION_ENDPOINT);
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}

	}

	private static List<Map<String, Object>> getSubscriptions(List<SubscriptionRequest> subRequests) {
		List<Map<String, Object>> result = Lists.newArrayList();
		for (SubscriptionRequest subRequest : subRequests) {
			result.add(subRequest.getSubscription().toJson());
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
					subscriptionService.getSubscription(id, headers).getSubscription().toJsonString(),
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
			if (bodyContext != null) {
				if (bodyContext instanceof List) {
					context.addAll((List<Object>) bodyContext);
				} else {
					context.add(bodyContext);
				}
			}
			Subscription subscription = Subscription.expandSubscription(body,
					JsonLdProcessor.getCoreContextClone().parse(context, true), true);
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
		Object geoProperty = map.get(NGSIConstants.NGSI_LD_GEOPROPERTY_GEOQ_ATTRIB);
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
					} else {
						myValue = SerializationTools.getProperLat(myValue);
					}
					result.add(myValue);
					lon = !lon;
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
