package eu.neclab.ngsildbroker.commons.tools;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLDService;
import com.github.jsonldjava.core.JsonLdConsts;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.EntityInfo;
import eu.neclab.ngsildbroker.commons.datatypes.NotificationParam;
import eu.neclab.ngsildbroker.commons.datatypes.QueryInfos;
import eu.neclab.ngsildbroker.commons.datatypes.QueryRemoteHost;
import eu.neclab.ngsildbroker.commons.datatypes.RegistrationEntry;
import eu.neclab.ngsildbroker.commons.datatypes.RemoteHost;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.SubscriptionRemoteHost;
import eu.neclab.ngsildbroker.commons.datatypes.ViaHeaders;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.InternalNotification;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AttrsQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.DataSetIdTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.GeoQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.LanguageQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.QQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.ScopeQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TypeQueryTerm;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.enums.Format;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple4;
import io.vertx.core.http.impl.headers.HeadersMultiMap;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.MultiMap;
import io.vertx.mutiny.ext.web.client.WebClient;
import jakarta.ws.rs.core.HttpHeaders;

import org.locationtech.spatial4j.SpatialPredicate;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.ShapeFactory.LineStringBuilder;
import org.locationtech.spatial4j.shape.ShapeFactory.MultiLineStringBuilder;
import org.locationtech.spatial4j.shape.ShapeFactory.MultiPointBuilder;
import org.locationtech.spatial4j.shape.ShapeFactory.MultiPolygonBuilder;
import org.locationtech.spatial4j.shape.ShapeFactory.PolygonBuilder;
import org.locationtech.spatial4j.shape.jts.JtsShapeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

@SuppressWarnings("unchecked")
public class SubscriptionTools {
	public static JtsShapeFactory shapeFactory = JtsSpatialContext.GEO.getShapeFactory();

	private final static Logger logger = LoggerFactory.getLogger(SubscriptionTools.class);

	public static boolean evaluateGeoQuery(GeoQueryTerm geoQuery, List<Map<String, Object>> locationsGeoProps) {
		if (geoQuery == null) {
			return true;
		}
		if (locationsGeoProps == null) {
			return false;
		}
		boolean result = false;
		for (Map<String, Object> locationGeoProp : locationsGeoProps) {
			List<Map<String, Object>> locations = (List<Map<String, Object>>) locationGeoProp
					.get(NGSIConstants.NGSI_LD_HAS_VALUE);
			for (Map<String, Object> location : locations) {
				String relation = geoQuery.getGeorel();
//				String regCoordinatesAsString = Subscription
//						.getCoordinates((List<Map<String, Object>>) location.get(NGSIConstants.NGSI_LD_COORDINATES));

				Shape queryShape;
				List<List<Number>> tmp;
				switch (geoQuery.getGeometry()) {
				case NGSIConstants.GEO_TYPE_POINT:
					queryShape = shapeFactory.pointXY((Double) geoQuery.getCoordinatesAsList().get(0),
							(Double) geoQuery.getCoordinatesAsList().get(1));
					break;
				case NGSIConstants.GEO_TYPE_LINESTRING:
					LineStringBuilder lineStringBuilder = shapeFactory.lineString();
					List<Object> lineList = geoQuery.getCoordinatesAsList();
					for (Object pointObj : lineList) {
						List<Number> point = (List<Number>) pointObj;
						lineStringBuilder.pointXY(point.get(0).doubleValue(), point.get(1).doubleValue());
					}
					queryShape = lineStringBuilder.build();
					break;
				case NGSIConstants.GEO_TYPE_POLYGON:
					PolygonBuilder polygonBuilder = shapeFactory.polygon();
					tmp = ((List<List<Number>>) geoQuery.getCoordinatesAsList().get(0));
					for (List<Number> point : tmp) {
						polygonBuilder.pointXY(point.get(0).doubleValue(), point.get(1).doubleValue());
					}
					queryShape = polygonBuilder.build();
					break;
				case NGSIConstants.GEO_TYPE_MULTI_POLYGON:
					MultiPolygonBuilder multiPolyBuilder = shapeFactory.multiPolygon();
					List<List<List<Number>>> coordList = ((List<List<List<Number>>>) geoQuery.getCoordinatesAsList()
							.get(0));
					for (List<List<Number>> tmp1 : coordList) {
						PolygonBuilder polygonBuilder1 = shapeFactory.polygon();
						for (List<Number> point : tmp1) {
							polygonBuilder1.pointXY(point.get(0).doubleValue(), point.get(1).doubleValue());
						}
						multiPolyBuilder.add(polygonBuilder1);

					}
					queryShape = multiPolyBuilder.build();
					break;
				case NGSIConstants.GEO_TYPE_MULTI_POINT:
					MultiPointBuilder multiPointBuilder = shapeFactory.multiPoint();
					List<Object> pointList = geoQuery.getCoordinatesAsList();
					for (Object pointObj : pointList) {
						List<Number> point = (List<Number>) pointObj;
						multiPointBuilder.pointXY(point.get(0).doubleValue(), point.get(1).doubleValue());
					}
					queryShape = multiPointBuilder.build();
					break;
				case NGSIConstants.GEO_TYPE_MULTI_LINESTRING:
					MultiLineStringBuilder multiLineStringBuilder = shapeFactory.multiLineString();
					List<Object> multiLineList = geoQuery.getCoordinatesAsList();
					for (Object lineObj : multiLineList) {

						LineStringBuilder lineStringBuilder1 = shapeFactory.lineString();
						List<Object> lineList1 = (List<Object>) lineObj;
						for (Object pointObj : lineList1) {
							List<Number> point = (List<Number>) pointObj;
							lineStringBuilder1.pointXY(point.get(0).doubleValue(), point.get(1).doubleValue());
						}
						multiLineStringBuilder.add(lineStringBuilder1);
					}
					queryShape = multiLineStringBuilder.build();
					break;
				default:
					logger.error(
							"Unsupported GeoJson type. Currently Point, Polygon and Linestring are supported but was "
									+ geoQuery.getGeometry());
					return false;

				}
				Shape entityShape;

				switch (((List<String>) location.get(NGSIConstants.JSON_LD_TYPE)).get(0)) {
				case NGSIConstants.NGSI_LD_POINT:
					List<Map<String, List<Map<String, Number>>>> coordinates = ((List<Map<String, List<Map<String, Number>>>>) location
							.get(NGSIConstants.NGSI_LD_COORDINATES));
					entityShape = shapeFactory.pointXY(
							coordinates.get(0).get(NGSIConstants.JSON_LD_LIST).get(0).get(NGSIConstants.JSON_LD_VALUE)
									.doubleValue(),
							coordinates.get(0).get(NGSIConstants.JSON_LD_LIST).get(1).get(NGSIConstants.JSON_LD_VALUE)
									.doubleValue());
					break;
				case NGSIConstants.NGSI_LD_LINESTRING:
					LineStringBuilder lineStringBuilder = shapeFactory.lineString();
					List<Map<String, List<Map<String, List<Map<String, Number>>>>>> linecoordinates = ((List<Map<String, List<Map<String, List<Map<String, Number>>>>>>) location
							.get(NGSIConstants.NGSI_LD_COORDINATES));
					for (Map<String, List<Map<String, Number>>> point : linecoordinates.get(0)
							.get(NGSIConstants.JSON_LD_LIST)) {
						lineStringBuilder.pointXY(
								point.get(NGSIConstants.JSON_LD_LIST).get(0).get(NGSIConstants.JSON_LD_VALUE)
										.doubleValue(),
								point.get(NGSIConstants.JSON_LD_LIST).get(1).get(NGSIConstants.JSON_LD_VALUE)
										.doubleValue());
					}
					entityShape = lineStringBuilder.build();
					break;
				case NGSIConstants.NGSI_LD_POLYGON:
					PolygonBuilder polygonBuilder = shapeFactory.polygon();
					List<Map<String, List<Map<String, List<Map<String, List<Map<String, Number>>>>>>>> polyogonCoordinates = ((List<Map<String, List<Map<String, List<Map<String, List<Map<String, Number>>>>>>>>) location
							.get(NGSIConstants.NGSI_LD_COORDINATES));
					for (Map<String, List<Map<String, Number>>> point : polyogonCoordinates.get(0)
							.get(NGSIConstants.JSON_LD_LIST).get(0).get(NGSIConstants.JSON_LD_LIST)) {
						polygonBuilder.pointXY(
								point.get(NGSIConstants.JSON_LD_LIST).get(0).get(NGSIConstants.JSON_LD_VALUE)
										.doubleValue(),
								point.get(NGSIConstants.JSON_LD_LIST).get(1).get(NGSIConstants.JSON_LD_VALUE)
										.doubleValue());
					}
					entityShape = polygonBuilder.build();
					break;
				case NGSIConstants.NGSI_LD_MULTI_POINT:
					MultiPointBuilder multiPointBuilder = shapeFactory.multiPoint();
					List<Map<String, List<Map<String, List<Map<String, Number>>>>>> multiPointCoordinates = ((List<Map<String, List<Map<String, List<Map<String, Number>>>>>>) location
							.get(NGSIConstants.NGSI_LD_COORDINATES));
					for (Map<String, List<Map<String, Number>>> point : multiPointCoordinates.get(0)
							.get(NGSIConstants.JSON_LD_LIST)) {
						multiPointBuilder.pointXY(
								point.get(NGSIConstants.JSON_LD_LIST).get(0).get(NGSIConstants.JSON_LD_VALUE)
										.doubleValue(),
								point.get(NGSIConstants.JSON_LD_LIST).get(1).get(NGSIConstants.JSON_LD_VALUE)
										.doubleValue());
					}
					entityShape = multiPointBuilder.build();
					break;
				case NGSIConstants.NGSI_LD_MULTI_LINESTRING:
					MultiLineStringBuilder multiLineStringBuilder = shapeFactory.multiLineString();
					List<Map<String, List<Map<String, List<Map<String, List<Map<String, Number>>>>>>>> multiLineCoordinates = (List<Map<String, List<Map<String, List<Map<String, List<Map<String, Number>>>>>>>>) location
							.get(NGSIConstants.NGSI_LD_COORDINATES);
					for (Map<String, List<Map<String, List<Map<String, Number>>>>> lineCoordinatesMap : multiLineCoordinates
							.get(0).get(NGSIConstants.JSON_LD_LIST)) {
						List<Map<String, List<Map<String, Number>>>> linecoordinates1 = lineCoordinatesMap
								.get(NGSIConstants.JSON_LD_LIST);
						LineStringBuilder lineStringBuilder1 = shapeFactory.lineString();
						for (Map<String, List<Map<String, Number>>> point : linecoordinates1) {
							lineStringBuilder1.pointXY(
									point.get(NGSIConstants.JSON_LD_LIST).get(0).get(NGSIConstants.JSON_LD_VALUE)
											.doubleValue(),
									point.get(NGSIConstants.JSON_LD_LIST).get(1).get(NGSIConstants.JSON_LD_VALUE)
											.doubleValue());
						}
						multiLineStringBuilder.add(lineStringBuilder1);
					}
					entityShape = multiLineStringBuilder.build();
					break;
				case NGSIConstants.NGSI_LD_MULTI_POLYGON:
					MultiPolygonBuilder multiPolygonBuilder = shapeFactory.multiPolygon();
					List<Map<String, List<Map<String, List<Map<String, List<Map<String, List<Map<String, Number>>>>>>>>>> multiPolyogonCoordinates = (List<Map<String, List<Map<String, List<Map<String, List<Map<String, List<Map<String, Number>>>>>>>>>>) location
							.get(NGSIConstants.NGSI_LD_COORDINATES);
					for (Map<String, List<Map<String, List<Map<String, List<Map<String, Number>>>>>>> polyMap : multiPolyogonCoordinates
							.get(0).get(NGSIConstants.JSON_LD_LIST)) {
						PolygonBuilder polygonBuilder1 = shapeFactory.polygon();
						List<Map<String, List<Map<String, List<Map<String, Number>>>>>> polyogonCoordinates1 = polyMap
								.get(NGSIConstants.JSON_LD_LIST);
						for (Map<String, List<Map<String, Number>>> point : polyogonCoordinates1.get(0)
								.get(NGSIConstants.JSON_LD_LIST)) {
							polygonBuilder1.pointXY(
									point.get(NGSIConstants.JSON_LD_LIST).get(0).get(NGSIConstants.JSON_LD_VALUE)
											.doubleValue(),
									point.get(NGSIConstants.JSON_LD_LIST).get(1).get(NGSIConstants.JSON_LD_VALUE)
											.doubleValue());
						}
						multiPolygonBuilder.add(polygonBuilder1);
					}
					entityShape = multiPolygonBuilder.build();
					break;
				default:
					logger.error(
							"Unsupported GeoJson type. Currently Point, Polygon and Linestring are supported but was "
									+ geoQuery.getGeometry());
					return false;

				}

				switch (relation) {
				case NGSIConstants.GEO_REL_EQUALS:
					result = SpatialPredicate.IsEqualTo.evaluate(entityShape, queryShape);
					break;
				case NGSIConstants.GEO_REL_NEAR:
					if (geoQuery.getDistanceType() == null) {
						result = SpatialPredicate.IsEqualTo.evaluate(entityShape, queryShape);
					}
					Shape bufferedShape;
					switch (geoQuery.getDistanceType()) {
					case NGSIConstants.GEO_REL_MAX_DISTANCE:
						bufferedShape = queryShape.getBuffered(geoQuery.getDistanceValue() * DistanceUtils.KM_TO_DEG,
								queryShape.getContext());
						result = SpatialPredicate.IsWithin.evaluate(entityShape, bufferedShape);
						break;
					case NGSIConstants.GEO_REL_MIN_DISTANCE:
						bufferedShape = queryShape.getBuffered(geoQuery.getDistanceValue() * DistanceUtils.KM_TO_DEG,
								queryShape.getContext());
						result = !SpatialPredicate.IsWithin.evaluate(entityShape, bufferedShape);
						break;
					default:
						result = false;
						break;
					}
					break;
				case NGSIConstants.GEO_REL_WITHIN:
					result = SpatialPredicate.IsWithin.evaluate(entityShape, queryShape);
					break;
				case NGSIConstants.GEO_REL_CONTAINS:
					result = SpatialPredicate.Contains.evaluate(entityShape, queryShape);
					break;
				case NGSIConstants.GEO_REL_INTERSECTS:
					result = SpatialPredicate.Intersects.evaluate(entityShape, queryShape);
					break;
				case NGSIConstants.GEO_REL_DISJOINT:
					result = SpatialPredicate.IsDisjointTo.evaluate(entityShape, queryShape);
					break;
				case NGSIConstants.GEO_REL_OVERLAPS:
					result = SpatialPredicate.Overlaps.evaluate(entityShape, queryShape);
					break;
				default:
					result = false;
					break;

				}
				if (result) {
					return result;
				}
			}
		}
		return result;
	}

	public static Uni<Map<String, Object>> generateNotification(SubscriptionRequest potentialSub, Object entity,
			JsonLDService ldService) {
		Set<String> options = new HashSet<>();
		if (potentialSub.getSubscription().getNotification().getSysAttrs()) {
			options.add(NGSIConstants.QUERY_PARAMETER_OPTIONS_SYSATTRS);
		}
		options.add(potentialSub.getSubscription().getNotification().getFormat().toString());
		return getContextForNotification(ldService, potentialSub).onItem().transformToUni(context -> {
			return ldService.compact(entity, null, context, HttpUtils.opts, -1, options, null).onItem()
					.transform(compacted -> {
						Map<String, Object> notification = Maps.newLinkedHashMap();
						notification.put(NGSIConstants.QUERY_PARAMETER_ID,
								"notification:" + UUID.randomUUID().getLeastSignificantBits());
						notification.put(NGSIConstants.QUERY_PARAMETER_TYPE, NGSIConstants.NOTIFICATION);
						notification.put(NGSIConstants.NGSI_LD_SUBSCRIPTION_ID_SHORT, potentialSub.getId());
						notification.put(NGSIConstants.NGSI_LD_NOTIFIED_AT_SHORT,
								SerializationTools.notifiedAt_formatter.format(LocalDateTime
										.ofInstant(Instant.ofEpochMilli(System.currentTimeMillis()), ZoneId.of("Z"))));
						List<Map<String, Object>> data = (List<Map<String, Object>>) compacted
								.getOrDefault(JsonLdConsts.GRAPH, List.of(compacted));
						int acceptHeader = HttpUtils.parseAcceptHeader(
								List.of(potentialSub.getSubscription().getNotification().getEndPoint().getAccept()));
						if (potentialSub.getSubscription().getNotification().getFormat() == Format.concise) {
							HttpUtils.makeConcise(compacted);
						}
						switch (acceptHeader) {
						case 1:
							data.forEach(entry -> entry.remove(NGSIConstants.JSON_LD_CONTEXT));
							notification.put(NGSIConstants.NGSI_LD_DATA_SHORT, data);
							break;
						case 2:
							// ld+
							data.forEach(entry -> entry.put(NGSIConstants.JSON_LD_CONTEXT,
									Collections.singletonList(potentialSub.getSubscription().getJsonldContext())));
							notification.put(NGSIConstants.NGSI_LD_DATA_SHORT, data);
							break;
						case 3:
							break;
						case 4:// geo+json
							List<String> atCtx = context.getOriginalAtContext();
							if (atCtx == null || atCtx.isEmpty()) {
								atCtx = List.of(NGSIConstants.CURRENT_CORE_CONTEXT);
							}
							try {
								notification.put(NGSIConstants.NGSI_LD_DATA_SHORT,
										HttpUtils.generateGeoJson(data, null, atCtx));
							} catch (ResponseException e) {
								logger.error("Failed to generate geo+json for subscription");
							}
							break;
						default:
							break;
						}

						return notification;
					});
		});
	}

	public static Uni<Context> getContextForNotification(JsonLDService ldService, SubscriptionRequest potentialSub) {
		if (potentialSub.getPayload().containsKey(NGSIConstants.NGSI_LD_JSONLD_CONTEXT)) {
			Object payloadAtContext = ((List<Map<String, Object>>) potentialSub.getPayload()
					.get(NGSIConstants.NGSI_LD_JSONLD_CONTEXT)).get(0).get(JsonLdConsts.VALUE);
			return ldService.parse(payloadAtContext);
		}
		return Uni.createFrom().item(potentialSub.getContext());
	}

	public static Uni<Map<String, Object>> generateCsourceNotification(SubscriptionRequest potentialSub, Object reg,
			int triggerReason, JsonLDService ldService) {
		return ldService.compact(reg, null, potentialSub.getContext(), HttpUtils.opts, -1).onItem()
				.transform(compacted -> {
					Map<String, Object> notification = Maps.newLinkedHashMap();
					notification.put(NGSIConstants.QUERY_PARAMETER_ID,
							"csourcenotification:" + UUID.randomUUID().getLeastSignificantBits());
					notification.put(NGSIConstants.QUERY_PARAMETER_TYPE, NGSIConstants.NOTIFICATION);
					notification.put(NGSIConstants.NGSI_LD_SUBSCRIPTION_ID_SHORT, potentialSub.getId());
					notification.put(NGSIConstants.NGSI_LD_NOTIFIED_AT_SHORT,
							SerializationTools.notifiedAt_formatter.format(LocalDateTime
									.ofInstant(Instant.ofEpochMilli(System.currentTimeMillis()), ZoneId.of("Z"))));

					notification.put(NGSIConstants.NGSI_LD_DATA_SHORT,
							compacted.getOrDefault(JsonLdConsts.GRAPH, List.of(compacted)));
					notification.put(NGSIConstants.NGSI_LD_TRIGGER_REASON_SHORT,
							HttpUtils.getTriggerReason(triggerReason));
					return notification;
				});
	}

	public static MultiMap getHeaders(NotificationParam notificationParam, HeadersMultiMap otherHead) {
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
		if (accept.equals(AppConstants.NGB_APPLICATION_JSON)) {
			result.addAll(otherHead);
		} else {
			result.addAll(otherHead);
			result.remove(NGSIConstants.LINK_HEADER);
		}
		result.set(HttpHeaders.ACCEPT, accept);
		result.set(HttpHeaders.CONTENT_TYPE, accept);
		return new MultiMap(result);
	}

	public static String getMqttPayload(NotificationParam notificationParam, Map<String, Object> notification)
			throws Exception {
		Map<String, Object> result = Maps.newLinkedHashMap();
		if (notificationParam.getEndPoint().getReceiverInfo() != null
				&& !notificationParam.getEndPoint().getReceiverInfo().isEmpty()) {
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

	public static Collection<SubscriptionRemoteHost> getRemoteSubscriptions(Subscription sub,
			List<EntityInfo> idsAndTypeQueryAndIdPattern, AttrsQueryTerm attrsQuery, QQueryTerm qQuery,
			GeoQueryTerm geoQuery, ScopeQueryTerm scopeQuery, LanguageQueryTerm langQuery,
			Collection<List<RegistrationEntry>> regEntries, Context context, ViaHeaders viaHeaders) {

		// ids, types, attrs, geo, scope
		List<Map<SubscriptionRemoteHost, QueryInfos>> remoteHost2QueryInfos = Lists.newArrayList();
		if (idsAndTypeQueryAndIdPattern == null) {
			idsAndTypeQueryAndIdPattern = Lists.newArrayList();
			idsAndTypeQueryAndIdPattern.add(new EntityInfo());
		}
		for (EntityInfo t : idsAndTypeQueryAndIdPattern) {
			Map<SubscriptionRemoteHost, QueryInfos> remoteHost2QueryInfo = Maps.newHashMap();
			remoteHost2QueryInfos.add(remoteHost2QueryInfo);
			Iterator<List<RegistrationEntry>> it = regEntries.iterator();
			String[] id = t.getId();
			TypeQueryTerm typeQuery = t.getTypeTerm();
			String idPattern = t.getIdPattern();
			while (it.hasNext()) {
				Iterator<RegistrationEntry> tenantRegs = it.next().iterator();
				while (tenantRegs.hasNext()) {

					RegistrationEntry regEntry = tenantRegs.next();
					if (regEntry.expiresAt() > 0 && regEntry.expiresAt() <= System.currentTimeMillis()) {
						it.remove();
						continue;
					}

					if (regEntry.matches(id, idPattern, typeQuery, attrsQuery, qQuery, geoQuery, scopeQuery) == null) {
						continue;
					}

					RemoteHost regHost = regEntry.host();
					if (viaHeaders.getHostUrls().contains(regHost.host())) {
						continue;
					}

					SubscriptionRemoteHost hostToQuery = SubscriptionRemoteHost
							.fromQueryRemoteHost(QueryRemoteHost.fromRegEntry(regEntry));
					QueryInfos queryInfos = remoteHost2QueryInfo.get(hostToQuery);
					if (queryInfos == null) {
						queryInfos = new QueryInfos();
						queryInfos.setGeoQuery(geoQuery);
						queryInfos.setLangQuery(langQuery);
						queryInfos.setTypeQuery(typeQuery);
						remoteHost2QueryInfo.put(hostToQuery, queryInfos);
					}

					if (!queryInfos.isFullIdFound()) {
						if (regEntry.eId() != null) {
							queryInfos.getIds().add(regEntry.eId());
						} else {
							if (id != null) {
								queryInfos.setIds(Sets.newHashSet(id));
								queryInfos.setFullIdFound(true);
							} else if (idPattern != null) {
								queryInfos.setIdPattern(idPattern);
							}
						}
					}
					if (!queryInfos.isFullTypesFound()) {
						if (regEntry.type() != null) {
							queryInfos.getTypes().add(regEntry.type());
						} else {
							if (typeQuery != null) {
								queryInfos.setTypes(typeQuery.getAllTypes());
								queryInfos.setFullTypesFound(true);
							}
						}
					}
					if (!queryInfos.isFullAttrsFound()) {
						if (regEntry.eProp() != null) {
							queryInfos.getAttrs().add(regEntry.eProp());
						} else if (regEntry.eRel() != null) {
							queryInfos.getAttrs().add(regEntry.eRel());
						} else {
							queryInfos.setFullAttrsFound(true);
							if (attrsQuery != null && attrsQuery.getAttrs() != null
									&& !attrsQuery.getAttrs().isEmpty()) {
								queryInfos.setAttrs(attrsQuery.getAttrs());
							}
						}
					}
				}

			}
		}
		Map<String, SubscriptionRemoteHost> cSourceId2QueryRemoteHost = Maps.newHashMap();

		for (Map<SubscriptionRemoteHost, QueryInfos> remoteHost2QueryInfo : remoteHost2QueryInfos) {
			for (Entry<SubscriptionRemoteHost, QueryInfos> entry : remoteHost2QueryInfo.entrySet()) {
				SubscriptionRemoteHost tmpHost = entry.getKey();
				SubscriptionRemoteHost finalHost = cSourceId2QueryRemoteHost.get(tmpHost.cSourceId());
				if (finalHost == null) {
					finalHost = tmpHost;
					viaHeaders.addViaHeader(tmpHost.host());
					finalHost.setViaHeaders(viaHeaders);
					cSourceId2QueryRemoteHost.put(finalHost.cSourceId(), finalHost);
				}

				Context contextToUse = finalHost.context();
				if (contextToUse == null) {
					finalHost.setContext(context);
					contextToUse = context;
				}
				Map<String, Object> queryParams = Maps
						.newHashMap(entry.getValue().toQueryParams(context, false, null, finalHost));

				Map<String, String> entities = Maps.newHashMap();
				queryParams.put(NGSIConstants.NGSI_LD_ENTITIES_SHORT, Lists.newArrayList(entities));
				if (queryParams.containsKey(NGSIConstants.ID)) {
					entities.put(NGSIConstants.ID, (String) queryParams.remove(NGSIConstants.ID));
				}
				if (queryParams.containsKey(NGSIConstants.TYPE)) {
					entities.put(NGSIConstants.TYPE, (String) queryParams.remove(NGSIConstants.TYPE));
				}
				if (queryParams.containsKey(NGSIConstants.QUERY_PARAMETER_IDPATTERN)) {
					entities.put(NGSIConstants.QUERY_PARAMETER_IDPATTERN,
							(String) queryParams.remove(NGSIConstants.QUERY_PARAMETER_IDPATTERN));
				}

				Set<String> watchedAttribs = sub.getAttributeNames();
				if (watchedAttribs != null && !watchedAttribs.isEmpty()) {
					List<String> compactedWatched = new ArrayList<>(watchedAttribs.size());
					watchedAttribs.forEach(attrib -> {
						compactedWatched.add(context.compactIri(attrib));
					});
					queryParams.put(NGSIConstants.NGSI_LD_WATCHED_ATTRIBUTES_SHORT, compactedWatched);
				}

				Set<String> notificationTrigger = sub.getNotificationTrigger();
				if (notificationTrigger != null && !notificationTrigger.isEmpty()) {
					queryParams.put(NGSIConstants.NGSI_LD_NOTIFICATION_TRIGGER_SHORT, notificationTrigger);
				}
				DataSetIdTerm datasetIdTerm = sub.getDatasetIdTerm();
				if (datasetIdTerm != null) {
					Set<String> datasetIds = sub.getDatasetIdTerm().getIds();
					if (datasetIds != null && !datasetIds.isEmpty()) {
						queryParams.put(NGSIConstants.NGSI_LD_DATA_SET_ID_SHORT, datasetIds);
					}
				}
				Map<String, Object> notificationParam = Maps.newHashMap();
				queryParams.put(NGSIConstants.NGSI_LD_NOTIFICATION_SHORT, notificationParam);
				Object attrs = queryParams.remove(NGSIConstants.QUERY_PARAMETER_ATTRS);
				if (attrs != null && attrs instanceof String s) {
					// nothing for now
				}
				notificationParam.put(NGSIConstants.QUERY_PARAMETER_OPTIONS_SYSATTRS, true);
				notificationParam.put(NGSIConstants.NGSI_LD_SHOWCHANGES_SHORT, true);
				notificationParam.put(NGSIConstants.NGSI_LD_FORMAT_SHORT, "normalized");
				queryParams.put(NGSIConstants.JSON_LD_CONTEXT, context.getOriginalAtContext());
				queryParams.put(NGSIConstants.TYPE, NGSIConstants.NGSI_LD_SUBSCRIPTION_SHORT);
				finalHost.setSubParam(queryParams);

			}
		}

		return cSourceId2QueryRemoteHost.values();
	}

	public static SubscriptionRequest generateRemoteSubscription(SubscriptionRequest subscriptionRequest,
			InternalNotification message) throws ResponseException {
		Map<String, Object> registryEntry = message.getPayload();
		String tenant;
		if (registryEntry.containsKey(NGSIConstants.NGSI_LD_TENANT)) {
			tenant = ((List<Map<String, String>>) registryEntry.get(NGSIConstants.NGSI_LD_TENANT)).get(0)
					.get(NGSIConstants.JSON_LD_VALUE);
		} else {
			tenant = AppConstants.INTERNAL_NULL_KEY;
		}
		Map<String, Object> newSub = MicroServiceUtils.deepCopyMap(subscriptionRequest.getPayload());
		clearEntitiesAndWatchedAttribs(newSub, registryEntry);
		clearGeoQuery(newSub, registryEntry);

		SubscriptionRequest result = new SubscriptionRequest(subscriptionRequest.getTenant(), newSub,
				subscriptionRequest.getContext());
		if (result.getSubscription().getNotification().getEndPoint().getReceiverInfo() != null)
			result.getSubscription().getNotification().getEndPoint().getReceiverInfo()
					.removeAll(NGSIConstants.TENANT_HEADER);
		if (!tenant.equals(AppConstants.INTERNAL_NULL_KEY)) {
			result.getSubscription().getNotification().getEndPoint().getReceiverInfo().put(NGSIConstants.TENANT_HEADER,
					tenant);
		}
		return result;
	}

	private static void clearGeoQuery(Map<String, Object> newSub, Map<String, Object> registryEntry)
			throws ResponseException {
		if (!registryEntry.containsKey(NGSIConstants.NGSI_LD_LOCATION)) {
			return;
		}
		if (!newSub.containsKey(NGSIConstants.NGSI_LD_GEO_QUERY)) {
			newSub.put(NGSIConstants.NGSI_LD_GEO_QUERY, getGeoQFromReg(
					((List<Map<String, Object>>) registryEntry.get(NGSIConstants.NGSI_LD_LOCATION)).get(0)));
			return;
		}
		Shape regShape = getShape(
				((List<Map<String, Object>>) registryEntry.get(NGSIConstants.NGSI_LD_LOCATION)).get(0));
		Map<String, Object> queryLocation = ((List<Map<String, Object>>) newSub.get(NGSIConstants.NGSI_LD_GEO_QUERY))
				.get(0);
		Shape queryShape = getShape(queryLocation);
		boolean notWithin = false;
		if (queryLocation.containsKey(NGSIConstants.NGSI_LD_GEO_REL)) {
			String[] splitted = ((List<Map<String, String>>) queryLocation.get(NGSIConstants.NGSI_LD_GEO_REL)).get(0)
					.get(NGSIConstants.JSON_LD_VALUE).split(";");
			if (NGSIConstants.GEO_REL_NEAR.equals(splitted[0])) {
				String[] minMax = splitted[1].split("==");
				queryShape = queryShape.getBuffered(Double.parseDouble(minMax[1]) * DistanceUtils.KM_TO_DEG,
						queryShape.getContext());
				if (NGSIConstants.GEO_REL_MIN_DISTANCE.equals(minMax[0])) {
					notWithin = true;
				}
			}
		}
		switch (regShape.relate(queryShape)) {
		case CONTAINS:
			// nothing to change if the query is contained in the registry
			return;
		case DISJOINT:
			if (notWithin) {
				return;
			}
			throw new ResponseException(ErrorType.InternalError,
					"disregarding remote subscription because of disjoint locations");
		case INTERSECTS:
		case WITHIN:
			newSub.put(NGSIConstants.NGSI_LD_GEO_QUERY, getOverlap(regShape, queryShape, notWithin));
			return;
		}
	}

	private static Object getOverlap(Shape regShape, Shape queryShape, boolean notWithin) {
		// no idea at the moment how to do this.
		new com.vividsolutions.jts.geom.Polygon(null, null, null);
		return null;
	}

	public static Shape getShape(Map<String, Object> location) throws ResponseException {
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
			throw new ResponseException(ErrorType.InternalError, "unsupported geo type");

		}
		return entityShape;
	}

	private static List<Map<String, Object>> getGeoQFromReg(Map<String, Object> location) {
		Map<String, Object> result = Maps.newHashMap();
		result.put(NGSIConstants.NGSI_LD_COORDINATES, location.get(NGSIConstants.NGSI_LD_COORDINATES));
		result.put(NGSIConstants.NGSI_LD_GEOMETRY, location.get(NGSIConstants.NGSI_LD_GEOMETRY));

		if (NGSIConstants.GEO_TYPE_POINT
				.equals(((List<Map<String, String>>) location.get(NGSIConstants.NGSI_LD_GEOMETRY)).get(0)
						.get(NGSIConstants.JSON_LD_VALUE))) {
			result.put(NGSIConstants.NGSI_LD_GEO_REL,
					List.of(Map.of(NGSIConstants.JSON_LD_VALUE, NGSIConstants.GEO_REL_EQUALS)));
		} else {
			result.put(NGSIConstants.NGSI_LD_GEO_REL,
					List.of(Map.of(NGSIConstants.JSON_LD_VALUE, NGSIConstants.GEO_REL_WITHIN)));
		}
		return List.of(result);
	}

	private static void clearEntitiesAndWatchedAttribs(Map<String, Object> newSub, Map<String, Object> registryEntry) {
		List<Tuple4<String, String, String, String>> regTuples = Lists.newArrayList();
		List<Tuple4<String, String, String, String>> subTuples = Lists.newArrayList();

		List<Map<String, Object>> regInfos = (List<Map<String, Object>>) registryEntry
				.get(NGSIConstants.NGSI_LD_INFORMATION);
		if (regInfos != null)
			for (Map<String, Object> regInfo : regInfos) {
				if (regInfo.containsKey(NGSIConstants.NGSI_LD_ENTITIES)) {
					List<Map<String, Object>> entities = (List<Map<String, Object>>) regInfo
							.get(NGSIConstants.NGSI_LD_ENTITIES);
					for (Map<String, Object> entry : entities) {
						String id = null;
						String idPattern = null;
						String type = null;
						if (entry.containsKey(NGSIConstants.JSON_LD_ID)) {
							id = (String) entry.get(NGSIConstants.JSON_LD_ID);
						}
						if (entry.containsKey(NGSIConstants.JSON_LD_TYPE)) {
							type = ((List<String>) entry.get(NGSIConstants.JSON_LD_TYPE)).get(0);
						}
						if (entry.containsKey(NGSIConstants.NGSI_LD_ID_PATTERN)) {
							idPattern = ((List<Map<String, String>>) entry.get(NGSIConstants.NGSI_LD_ID_PATTERN)).get(0)
									.get(NGSIConstants.JSON_LD_VALUE);
						}
						if (regInfo.containsKey(NGSIConstants.NGSI_LD_RELATIONSHIPS)
								|| regInfo.containsKey(NGSIConstants.NGSI_LD_PROPERTIES)) {
							if (regInfo.containsKey(NGSIConstants.NGSI_LD_RELATIONSHIPS)) {
								for (Map<String, String> relationship : (List<Map<String, String>>) regInfo
										.get(NGSIConstants.NGSI_LD_RELATIONSHIPS)) {
									regTuples.add(
											Tuple4.of(id, idPattern, type, relationship.get(NGSIConstants.JSON_LD_ID)));
								}
							}
							if (regInfo.containsKey(NGSIConstants.NGSI_LD_PROPERTIES)) {
								for (Map<String, String> property : (List<Map<String, String>>) regInfo
										.get(NGSIConstants.NGSI_LD_PROPERTIES)) {
									regTuples.add(
											Tuple4.of(id, idPattern, type, property.get(NGSIConstants.JSON_LD_ID)));
								}
							}
						} else {
							regTuples.add(Tuple4.of(id, idPattern, type, null));
						}
					}
				} else {
					if (regInfo.containsKey(NGSIConstants.NGSI_LD_RELATIONSHIPS)) {
						for (Map<String, String> relationship : (List<Map<String, String>>) regInfo
								.get(NGSIConstants.NGSI_LD_RELATIONSHIPS)) {
							regTuples.add(Tuple4.of(null, null, null, relationship.get(NGSIConstants.JSON_LD_ID)));
						}
					}
					if (regInfo.containsKey(NGSIConstants.NGSI_LD_PROPERTIES)) {
						for (Map<String, String> property : (List<Map<String, String>>) regInfo
								.get(NGSIConstants.NGSI_LD_PROPERTIES)) {
							regTuples.add(Tuple4.of(null, null, null, property.get(NGSIConstants.JSON_LD_ID)));
						}
					}
				}
			}
		List<Map<String, Object>> subEntities;
		List<Map<String, String>> watchedAttribs;
		if (newSub.containsKey(NGSIConstants.NGSI_LD_ENTITIES)) {
			subEntities = (List<Map<String, Object>>) newSub.get(NGSIConstants.NGSI_LD_ENTITIES);
			for (Map<String, Object> entry : subEntities) {

				String id = null;
				String idPattern = null;
				String type = null;
				if (entry.containsKey(NGSIConstants.JSON_LD_ID)) {
					id = (String) entry.get(NGSIConstants.JSON_LD_ID);
				}
				if (entry.containsKey(NGSIConstants.JSON_LD_TYPE)) {
					type = ((List<String>) entry.get(NGSIConstants.JSON_LD_TYPE)).get(0);
				}
				if (entry.containsKey(NGSIConstants.NGSI_LD_ID_PATTERN)) {
					idPattern = ((List<Map<String, String>>) entry.get(NGSIConstants.NGSI_LD_ID_PATTERN)).get(0)
							.get(NGSIConstants.JSON_LD_VALUE);
				}
				if (newSub.containsKey(NGSIConstants.NGSI_LD_WATCHED_ATTRIBUTES)) {
					watchedAttribs = (List<Map<String, String>>) newSub.get(NGSIConstants.NGSI_LD_WATCHED_ATTRIBUTES);
					for (Map<String, String> attrib : watchedAttribs) {
						subTuples.add(Tuple4.of(id, idPattern, type, attrib.get(NGSIConstants.JSON_LD_ID)));
					}
				} else {
					subTuples.add(Tuple4.of(id, idPattern, type, null));
				}
			}
		} else {
			watchedAttribs = (List<Map<String, String>>) newSub.get(NGSIConstants.NGSI_LD_WATCHED_ATTRIBUTES);
			for (Map<String, String> attrib : watchedAttribs) {
				subTuples.add(Tuple4.of(null, null, null, attrib.get(NGSIConstants.JSON_LD_ID)));
			}
		}
		// id, idpattern, type, attribname combo
		for (Tuple4<String, String, String, String> subTuple : subTuples) {
//			Tuple4<String, String, String, String> bestFit;
			for (Tuple4<String, String, String, String> regTuple : regTuples) {
				String id;// , idpattern, type, attribname;

				if (regTuple.getItem1() == null || (regTuple.getItem1() != null && subTuple.getItem1() == null)
						&& regTuple.getItem1().equals(subTuple.getItem1())) {
					id = subTuple.getItem1();
				} // else if(subTuple.getItem1())
			}
		}
	}

	public static Uni<Void> subsribeRemote(SubscriptionRemoteHost sub, WebClient webClient, String endpointUri) {
		Map<String, Object> endPoint = Maps.newHashMap();
		((Map<String, Object>) sub.getSubParam().get(NGSIConstants.NGSI_LD_NOTIFICATION_SHORT))
				.put(NGSIConstants.NGSI_LD_ENDPOINT_SHORT, endPoint);
		endPoint.put(NGSIConstants.NGSI_LD_URI_SHORT, endpointUri);
		endPoint.put(NGSIConstants.ACCEPT, AppConstants.NGB_APPLICATION_JSON);
		return webClient.postAbs(sub.host() + NGSIConstants.NGSI_LD_SUB_ENDPOINT + "/")
				.putHeader(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSONLD)
				.sendJsonObject(new JsonObject(sub.getSubParam())).onFailure().retry().atMost(3).onItem()
				.transformToUni(resp -> {
					if (resp.statusCode() == 201) {
						String locationHeader = resp.headers().get(HttpHeaders.LOCATION);
						sub.setSubscriptionId(locationHeader.substring(locationHeader.lastIndexOf('/') + 1));
					} else {
						logger.debug(resp.statusCode() + resp.statusMessage());
						logger.debug(resp.bodyAsString());
						logger.debug(resp.headers().toString());
					}
					return Uni.createFrom().voidItem();
				}).onFailure().recoverWithUni(e -> {
					logger.error("Failed to remote unsubscribe to " + sub);
					return Uni.createFrom().voidItem();
				});
	}

	public static Uni<Void> unsubsribeRemote(SubscriptionRemoteHost sub, WebClient webClient) {
		return webClient.deleteAbs(sub.host() + NGSIConstants.NGSI_LD_SUB_ENDPOINT + "/" + sub.getSubscriptionId())
				.send().onFailure().retry().atMost(3).onItem().transformToUni(resp -> Uni.createFrom().voidItem())
				.onFailure().recoverWithUni(e -> {
					logger.error("Failed to remote unsubscribe to " + sub);
					return Uni.createFrom().voidItem();
				});
	}

}
