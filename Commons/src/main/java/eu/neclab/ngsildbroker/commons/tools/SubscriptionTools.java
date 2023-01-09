package eu.neclab.ngsildbroker.commons.tools;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;

import org.locationtech.spatial4j.SpatialPredicate;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.ShapeFactory.LineStringBuilder;
import org.locationtech.spatial4j.shape.ShapeFactory.PolygonBuilder;
import org.locationtech.spatial4j.shape.jts.JtsShapeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.NotificationParam;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.terms.GeoQueryTerm;
import io.vertx.core.http.impl.headers.HeadersMultiMap;
import io.vertx.mutiny.core.MultiMap;

public class SubscriptionTools {
	private static JtsShapeFactory shapeFactory = JtsSpatialContext.GEO.getShapeFactory();
	private final static Logger logger = LoggerFactory.getLogger(SubscriptionTools.class);

	@SuppressWarnings("unchecked")
	public static boolean evaluateGeoQuery(GeoQueryTerm geoQuery, Map<String, Object> location) {
		if (geoQuery == null) {
			return true;
		}
		if (location == null) {
			return false;
		}
		String relation = geoQuery.getGeorel();
		String regCoordinatesAsString = Subscription
				.getCoordinates((List<Map<String, Object>>) location.get(NGSIConstants.NGSI_LD_COORDINATES));
		if (relation.equals(NGSIConstants.GEO_REL_EQUALS)) {
			return geoQuery.getCoordinates().equals(regCoordinatesAsString);
		}
		Shape queryShape;
		List<List<Double>> tmp;
		switch (geoQuery.getGeometry()) {
		case NGSIConstants.GEO_TYPE_POINT:
			queryShape = shapeFactory.pointXY((Double) geoQuery.getCoordinatesAsList().get(0),
					(Double) geoQuery.getCoordinatesAsList().get(1));
			break;
		case NGSIConstants.GEO_TYPE_LINESTRING:
			LineStringBuilder lineStringBuilder = shapeFactory.lineString();
			tmp = (List<List<Double>>) geoQuery.getCoordinatesAsList().get(0);
			for (List<Double> point : tmp) {
				lineStringBuilder.pointXY(point.get(0), point.get(1));
			}
			queryShape = lineStringBuilder.build();
			break;
		case NGSIConstants.GEO_TYPE_POLYGON:
			PolygonBuilder polygonBuilder = shapeFactory.polygon();
			tmp = ((List<List<List<Double>>>) geoQuery.getCoordinatesAsList().get(0)).get(0);
			for (List<Double> point : tmp) {
				polygonBuilder.pointXY(point.get(0), point.get(1));
			}
			queryShape = polygonBuilder.build();
			break;
		case NGSIConstants.GEO_TYPE_MULTI_POLYGON:
		default:
			logger.error("Unsupported GeoJson type. Currently Point, Polygon and Linestring are supported but was "
					+ geoQuery.getGeometry());
			return false;

		}
		Shape entityShape;

		switch (((List<String>) location.get(NGSIConstants.JSON_LD_TYPE)).get(0)) {
		case NGSIConstants.NGSI_LD_POINT:
			List<Map<String, List<Map<String, Double>>>> coordinates = ((List<Map<String, List<Map<String, Double>>>>) location
					.get(NGSIConstants.NGSI_LD_COORDINATES));
			entityShape = shapeFactory.pointXY(
					coordinates.get(0).get(NGSIConstants.JSON_LD_LIST).get(0).get(NGSIConstants.JSON_LD_VALUE),
					coordinates.get(0).get(NGSIConstants.JSON_LD_LIST).get(1).get(NGSIConstants.JSON_LD_VALUE));
			break;
		case NGSIConstants.NGSI_LD_LINESTRING:
			LineStringBuilder lineStringBuilder = shapeFactory.lineString();
			List<Map<String, List<Map<String, List<Map<String, Double>>>>>> linecoordinates = ((List<Map<String, List<Map<String, List<Map<String, Double>>>>>>) location
					.get(NGSIConstants.NGSI_LD_COORDINATES));
			for (Map<String, List<Map<String, Double>>> point : linecoordinates.get(0)
					.get(NGSIConstants.JSON_LD_LIST)) {
				lineStringBuilder.pointXY(point.get(NGSIConstants.JSON_LD_LIST).get(0).get(NGSIConstants.JSON_LD_VALUE),
						point.get(NGSIConstants.JSON_LD_LIST).get(1).get(NGSIConstants.JSON_LD_VALUE));
			}
			entityShape = lineStringBuilder.build();
			break;
		case NGSIConstants.NGSI_LD_POLYGON:
			PolygonBuilder polygonBuilder = shapeFactory.polygon();
			List<Map<String, List<Map<String, List<Map<String, List<Map<String, Double>>>>>>>> polyogonCoordinates = ((List<Map<String, List<Map<String, List<Map<String, List<Map<String, Double>>>>>>>>) location
					.get(NGSIConstants.NGSI_LD_COORDINATES));
			for (Map<String, List<Map<String, Double>>> point : polyogonCoordinates.get(0)
					.get(NGSIConstants.JSON_LD_LIST).get(0).get(NGSIConstants.JSON_LD_LIST)) {
				polygonBuilder.pointXY(point.get(NGSIConstants.JSON_LD_LIST).get(0).get(NGSIConstants.JSON_LD_VALUE),
						point.get(NGSIConstants.JSON_LD_LIST).get(1).get(NGSIConstants.JSON_LD_VALUE));
			}
			entityShape = polygonBuilder.build();
			break;
		case NGSIConstants.GEO_TYPE_MULTI_POLYGON:
		default:
			logger.error("Unsupported GeoJson type. Currently Point, Polygon and Linestring are supported but was "
					+ geoQuery.getGeometry());
			return false;

		}

		switch (relation) {
		case NGSIConstants.GEO_REL_NEAR:
			if (geoQuery.getDistanceType() == null) {
				return geoQuery.getCoordinates().equals(regCoordinatesAsString);
			}
			Shape bufferedShape;
			switch (geoQuery.getDistanceType()) {
			case NGSIConstants.GEO_REL_MAX_DISTANCE:
				bufferedShape = queryShape.getBuffered(geoQuery.getDistanceValue() * DistanceUtils.KM_TO_DEG,
						queryShape.getContext());
				return SpatialPredicate.IsWithin.evaluate(entityShape, bufferedShape);
			case NGSIConstants.GEO_REL_MIN_DISTANCE:
				bufferedShape = queryShape.getBuffered(geoQuery.getDistanceValue() * DistanceUtils.KM_TO_DEG,
						queryShape.getContext());
				return !SpatialPredicate.IsWithin.evaluate(entityShape, bufferedShape);
			default:
				return false;
			}
		case NGSIConstants.GEO_REL_WITHIN:
			return SpatialPredicate.IsWithin.evaluate(entityShape, queryShape);
		case NGSIConstants.GEO_REL_CONTAINS:
			return SpatialPredicate.Contains.evaluate(entityShape, queryShape);
		case NGSIConstants.GEO_REL_INTERSECTS:
			return SpatialPredicate.Intersects.evaluate(entityShape, queryShape);
		case NGSIConstants.GEO_REL_DISJOINT:
			return SpatialPredicate.IsDisjointTo.evaluate(entityShape, queryShape);
		case NGSIConstants.GEO_REL_OVERLAPS:
			return SpatialPredicate.Overlaps.evaluate(entityShape, queryShape);
		default:
			return false;

		}

	}
	
	public static Map<String, Object> generateNotification(SubscriptionRequest potentialSub, Object reg, int triggerReason)
			throws Exception {
		Map<String, Object> notification = Maps.newLinkedHashMap();
		notification.put(NGSIConstants.QUERY_PARAMETER_ID,
				"csourcenotification:" + UUID.randomUUID().getLeastSignificantBits());
		notification.put(NGSIConstants.QUERY_PARAMETER_TYPE, NGSIConstants.CSOURCE_NOTIFICATION);
		notification.put(NGSIConstants.NGSI_LD_SUBSCRIPTION_ID_SHORT, potentialSub.getId());
		notification.put(NGSIConstants.NGSI_LD_NOTIFIED_AT_SHORT, SerializationTools.notifiedAt_formatter
				.format(LocalDateTime.ofInstant(Instant.ofEpochMilli(System.currentTimeMillis()), ZoneId.of("Z"))));
		Map<String, Object> compacted = JsonLdProcessor.compact(reg, null, potentialSub.getContext(), HttpUtils.opts,
				-1);
		// TODO check what compacted produces here and add the correct things
		notification.put(NGSIConstants.NGSI_LD_DATA_SHORT, Lists.newArrayList(compacted));
		notification.put(NGSIConstants.NGSI_LD_TRIGGER_REASON_SHORT, HttpUtils.getTriggerReason(triggerReason));
		return notification;
	}
	
	public static MultiMap getHeaders(NotificationParam notificationParam) {
		HeadersMultiMap result = new HeadersMultiMap();

		ArrayListMultimap<String, String> receiverInfo = notificationParam.getEndPoint().getReceiverInfo();
		if (receiverInfo != null) {
			for (Entry<String, String> entry : receiverInfo.entries()) {
				result.add(entry.getKey(), entry.getValue());
			}
		}
		String accept = notificationParam.getEndPoint().getAccept();
		if (accept == null) {
			accept = AppConstants.NGB_APPLICATION_JSON;
		}
		result.set("accept", accept);
		return new MultiMap(result);
	}
	
	public static String getMqttPayload(NotificationParam notificationParam, Map<String, Object> notification)
			throws Exception {
		Map<String, Object> result = Maps.newLinkedHashMap();
		if (!notificationParam.getEndPoint().getReceiverInfo().isEmpty()) {
			result.put(NGSIConstants.METADATA, getMqttMetaData(notificationParam.getEndPoint().getReceiverInfo()));
		}
		result.put(NGSIConstants.BODY, notification);
		return JsonUtils.toString(result);
	}

	private static List<Map<String, String>> getMqttMetaData(ArrayListMultimap<String, String> receiverInfo) {
		List<Map<String, String>> result = Lists.newArrayList();
		for (Entry<String, String> entry : receiverInfo.entries()) {
			Map<String, String> tmp = new HashMap<>(1);
			tmp.put(entry.getKey(), entry.getValue());
			result.add(tmp);
		}
		return result;
	}
	public static void setInitTimesSentAndFailed(SubscriptionRequest request) {
		List<Map<String, Object>> timeValue = Lists.newArrayList();
		Map<String, Object> tmp = Maps.newHashMap();
		tmp.put(NGSIConstants.JSON_LD_VALUE, 0);
		timeValue.add(tmp);
		request.getPayload().put(NGSIConstants.NGSI_LD_TIMES_SENT, timeValue);
		request.getPayload().put(NGSIConstants.NGSI_LD_TIMES_FAILED, timeValue);
	}

}
