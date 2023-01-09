package eu.neclab.ngsildbroker.registry.subscriptionmanager.service;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.locationtech.spatial4j.SpatialPredicate;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.ShapeFactory.LineStringBuilder;
import org.locationtech.spatial4j.shape.ShapeFactory.PolygonBuilder;
import org.locationtech.spatial4j.shape.jts.JtsShapeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.common.collect.Table.Cell;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.EntityInfo;
import eu.neclab.ngsildbroker.commons.datatypes.NotificationParam;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.DeleteSubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.InternalNotification;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.UpdateSubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.results.NGSILDOperationResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.datatypes.terms.GeoQueryTerm;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;
import eu.neclab.ngsildbroker.registry.subscriptionmanager.repository.RegistrySubscriptionInfoDAO;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.annotations.Broadcast;
import io.vertx.core.http.impl.headers.HeadersMultiMap;
import io.vertx.mutiny.core.MultiMap;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.WebClient;
import io.vertx.mutiny.mqtt.MqttClient;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowIterator;

@Singleton
public class RegistrySubscriptionService {

	private final static Logger logger = LoggerFactory.getLogger(RegistrySubscriptionService.class);

	@Inject
	RegistrySubscriptionInfoDAO regDAO;

	@Inject
	@Channel(AppConstants.INTERNAL_NOTIFICATION_CHANNEL)
	@Broadcast
	MutinyEmitter<InternalNotification> internalNotificationSender;

	@Inject
	Vertx vertx;

	private Table<String, String, SubscriptionRequest> tenant2subscriptionId2Subscription = HashBasedTable.create();

	private Table<String, String, SubscriptionRequest> tenant2subscriptionId2IntervalSubscription = HashBasedTable
			.create();

	private WebClient webClient;

	private Map<String, MqttClient> host2MqttClient = Maps.newHashMap();

	private JtsShapeFactory shapeFactory = JtsSpatialContext.GEO.getShapeFactory();

	@PostConstruct
	Uni<Void> setup() {
		this.webClient = WebClient.create(vertx);

		return regDAO.loadSubscriptions().onItem().transformToUni(subs -> {
			subs.forEach(tuple -> {
				SubscriptionRequest request;
				try {
					request = new SubscriptionRequest(tuple.getItem1(), tuple.getItem2(),
							new Context().parse(tuple.getItem3(), false));
					if (isIntervalSub(request)) {
						this.tenant2subscriptionId2IntervalSubscription.put(request.getTenant(), request.getId(),
								request);
					} else {
						this.tenant2subscriptionId2Subscription.put(request.getTenant(), request.getId(), request);
					}

				} catch (Exception e) {
					logger.error("Failed to load stored subscription " + tuple.getItem1());
				}
			});
			return Uni.createFrom().voidItem();
		});

	}

	private boolean isIntervalSub(SubscriptionRequest request) {
		return request.getSubscription().getTimeInterval() > 0;
	}

	public Uni<NGSILDOperationResult> createSubscription(String tenant, Map<String, Object> subscription,
			Context context) {
		SubscriptionRequest request;
		try {
			request = new SubscriptionRequest(tenant, subscription, context);
		} catch (ResponseException e) {
			return Uni.createFrom().failure(e);
		}
		setInitTimesSentAndFailed(request);
		return regDAO.createSubscription(request).onItem().transformToUni(t -> {
			if (isIntervalSub(request)) {
				this.tenant2subscriptionId2IntervalSubscription.put(request.getTenant(), request.getId(), request);
			} else {
				tenant2subscriptionId2Subscription.put(tenant, request.getId(), request);
			}
			return regDAO.getInitialNotificationData(request).onItem().transformToUni(rows -> {
				List<Map<String, Object>> data = Lists.newArrayList();
				rows.forEach(row -> {
					data.add(row.getJsonObject(0).getMap());
				});
				try {
					return sendNotification(request,
							generateNotification(request, data, AppConstants.INTERNAL_NOTIFICATION_REQUEST),
							AppConstants.INTERNAL_NOTIFICATION_REQUEST).onItem().transform(v -> {
								return new NGSILDOperationResult(AppConstants.CREATE_SUBSCRIPTION_REQUEST,
										request.getId());
							});
				} catch (Exception e) {
					logger.error("Failed to send initial notifcation", e);
					return Uni.createFrom()
							.item(new NGSILDOperationResult(AppConstants.CREATE_SUBSCRIPTION_REQUEST, request.getId()));
				}
			});

		}).onFailure().recoverWithUni(e -> {
			// TODO sql check
			return Uni.createFrom().failure(new ResponseException(ErrorType.AlreadyExists,
					"Subscription with id " + request.getId() + " exists"));
		});
	}

	private void setInitTimesSentAndFailed(SubscriptionRequest request) {
		List<Map<String, Object>> timeValue = Lists.newArrayList();
		Map<String, Object> tmp = Maps.newHashMap();
		tmp.put(NGSIConstants.JSON_LD_VALUE, 0);
		timeValue.add(tmp);
		request.getPayload().put(NGSIConstants.NGSI_LD_TIMES_SENT, timeValue);
		request.getPayload().put(NGSIConstants.NGSI_LD_TIMES_FAILED, timeValue);
	}

	public Uni<NGSILDOperationResult> updateSubscription(String tenant, String subscriptionId,
			Map<String, Object> update, Context context) {
		UpdateSubscriptionRequest request = new UpdateSubscriptionRequest(tenant, subscriptionId, update, context);
		return regDAO.updateSubscription(request).onItem().transformToUni(t -> {
			if (t.rowCount() == 0) {
				return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound, "subscription not found"));
			}
			Row row = t.iterator().next();
			SubscriptionRequest updatedRequest;
			try {
				updatedRequest = new SubscriptionRequest(tenant, row.getJsonObject(0).getMap(),
						new Context().parse(row.getJsonObject(0).getMap(), false));
			} catch (Exception e) {
				return Uni.createFrom().failure(e);
			}

			if (isIntervalSub(updatedRequest)) {
				tenant2subscriptionId2IntervalSubscription.put(tenant, updatedRequest.getId(), updatedRequest);
				tenant2subscriptionId2Subscription.remove(tenant, updatedRequest.getId());
			} else {
				tenant2subscriptionId2Subscription.put(tenant, updatedRequest.getId(), updatedRequest);
				tenant2subscriptionId2IntervalSubscription.remove(tenant, updatedRequest.getId());
			}
			return Uni.createFrom()
					.item(new NGSILDOperationResult(AppConstants.UPDATE_SUBSCRIPTION_REQUEST, subscriptionId));
		});
	}

	public Uni<NGSILDOperationResult> deleteSubscription(String tenant, String subscriptionId) {
		DeleteSubscriptionRequest request = new DeleteSubscriptionRequest(tenant, subscriptionId);
		return regDAO.deleteSubscription(request).onItem().transform(t -> {
			tenant2subscriptionId2IntervalSubscription.remove(tenant, subscriptionId);
			tenant2subscriptionId2Subscription.remove(tenant, subscriptionId);
			return new NGSILDOperationResult(AppConstants.DELETE_SUBSCRIPTION_REQUEST, subscriptionId);
		});
	}

	public Uni<QueryResult> getAllSubscriptions(String tenant, int limit, int offset) {
		return regDAO.getAllSubscriptions(tenant, limit, offset).onItem().transform(rows -> {
			QueryResult result = new QueryResult();
			Row next = null;
			RowIterator<Row> it = rows.iterator();
			List<Map<String, Object>> resultData = new ArrayList<Map<String, Object>>(rows.size());
			while (it.hasNext()) {
				next = it.next();
				resultData.add(next.getJsonObject(1).getMap());
			}
			result.setData(resultData);
			if (next == null) {
				return result;
			}
			Long resultCount = next.getLong(0);
			result.setCount(resultCount);
			long leftAfter = resultCount - (offset + limit);
			if (leftAfter < 0) {
				leftAfter = 0;
			}
			long leftBefore = offset;
			result.setResultsLeftAfter(leftAfter);
			result.setResultsLeftBefore(leftBefore);
			result.setLimit(limit);
			result.setOffset(offset);
			return result;
		});
	}

	public Uni<Map<String, Object>> getSubscription(String tenant, String subscriptionId) {
		return regDAO.getSubscription(tenant, subscriptionId).onItem().transformToUni(rows -> {
			if (rows.size() == 0) {
				return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound, "subscription not found"));
			}
			return Uni.createFrom().item(rows.iterator().next().getJsonObject(0).getMap());
		});
	}

	public Uni<Void> checkSubscriptions(BaseRequest message) {
		Collection<SubscriptionRequest> potentialSubs = tenant2subscriptionId2Subscription.column(message.getTenant())
				.values();
		List<Uni<Void>> unis = Lists.newArrayList();
		for (SubscriptionRequest potentialSub : potentialSubs) {
			switch (message.getRequestType()) {
			case AppConstants.UPDATE_REQUEST:
				if (shouldFire(message.getPayload(), potentialSub)) {
					unis.add(regDAO.getRegById(message.getTenant(), message.getId()).onItem().transformToUni(rows -> {
						return sendNotification(potentialSub, rows.iterator().next().getJsonObject(0).getMap(),
								message.getRequestType());
					}));
				}
				break;
			case AppConstants.CREATE_REQUEST:
			case AppConstants.DELETE_REQUEST:
				unis.add(sendNotification(potentialSub, message.getPayload(), message.getRequestType()));
			default:
				break;
			}

		}
		return Uni.combine().all().unis(unis).discardItems();
	}

	private Uni<Void> sendNotification(SubscriptionRequest potentialSub, Map<String, Object> reg, int triggerReason) {
		if (shouldSendOut(potentialSub, reg)) {
			Map<String, Object> notification;
			try {
				notification = generateNotification(potentialSub, reg, triggerReason);
			} catch (Exception e) {
				logger.error("Failed to generate notification", e);
				return Uni.createFrom().voidItem();
			}
			NotificationParam notificationParam = potentialSub.getSubscription().getNotification();
			Uni<Void> toSend;
			switch (notificationParam.getEndPoint().getUri().getScheme()) {
			case "internal":
				toSend = internalNotificationSender
						.send(new InternalNotification(potentialSub.getTenant(), potentialSub.getId(), notification));
				break;
			case "mqtt":
			case "mqtts":
				try {
					toSend = getMqttClient(notificationParam).onItem().transformToUni(client -> {
						int qos = 1;

						String qosString = notificationParam.getEndPoint().getNotifierInfo()
								.get(NGSIConstants.MQTT_QOS);
						if (qosString != null) {
							qos = Integer.parseInt(qosString);
						}
						try {
							return client.publish(notificationParam.getEndPoint().getUri().getPath().substring(1),
									Buffer.buffer(getMqttPayload(notificationParam, notification)),
									MqttQoS.valueOf(qos), false, false).onItem().transformToUni(t -> {
										if (t == 0) {
											// TODO what the fuck is the result here
										}
										long now = System.currentTimeMillis();
										potentialSub.getSubscription().getNotification()
												.setLastSuccessfulNotification(now);
										potentialSub.getSubscription().getNotification().setLastNotification(now);
										return regDAO.updateNotificationSuccess(potentialSub.getTenant(),
												potentialSub.getId(),
												SerializationTools.notifiedAt_formatter.format(LocalDateTime
														.ofInstant(Instant.ofEpochMilli(now), ZoneId.of("Z"))));
									}).onFailure().recoverWithUni(e -> {
										logger.error(
												"failed to send notification for subscription " + potentialSub.getId(),
												e);
										long now = System.currentTimeMillis();
										potentialSub.getSubscription().getNotification().setLastFailedNotification(now);
										potentialSub.getSubscription().getNotification().setLastNotification(now);
										return regDAO.updateNotificationFailure(potentialSub.getTenant(),
												potentialSub.getId(),
												SerializationTools.notifiedAt_formatter.format(LocalDateTime
														.ofInstant(Instant.ofEpochMilli(now), ZoneId.of("Z"))));
									});
						} catch (Exception e) {
							logger.error("failed to send notification for subscription " + potentialSub.getId(), e);
							return Uni.createFrom().voidItem();
						}
					});
				} catch (Exception e) {
					logger.error("failed to send notification for subscription " + potentialSub.getId(), e);
					return Uni.createFrom().voidItem();
				}
				break;
			case "http":
			case "https":
				try {
					toSend = webClient.post(notificationParam.getEndPoint().getUri().toString())
							.putHeaders(getHeaders(notificationParam))
							.sendBuffer(Buffer.buffer(JsonUtils.toPrettyString(JsonLdProcessor.compact(notification,
									null, potentialSub.getContext(), HttpUtils.opts, -1))))
							.onFailure().retry().atMost(3).onItem().transformToUni(result -> {
								int statusCode = result.statusCode();
								long now = System.currentTimeMillis();
								if (statusCode > 200 && statusCode < 300) {
									potentialSub.getSubscription().getNotification().setLastSuccessfulNotification(now);
									potentialSub.getSubscription().getNotification().setLastNotification(now);
									return regDAO.updateNotificationSuccess(potentialSub.getTenant(),
											potentialSub.getId(),
											SerializationTools.notifiedAt_formatter.format(LocalDateTime
													.ofInstant(Instant.ofEpochMilli(now), ZoneId.of("Z"))));
								} else {
									logger.error("failed to send notification for subscription " + potentialSub.getId()
											+ " with status code " + statusCode
											+ ". Remember there is no redirect following for post due to security considerations");
									potentialSub.getSubscription().getNotification().setLastFailedNotification(now);
									potentialSub.getSubscription().getNotification().setLastNotification(now);
									return regDAO.updateNotificationFailure(potentialSub.getTenant(),
											potentialSub.getId(),
											SerializationTools.notifiedAt_formatter.format(LocalDateTime
													.ofInstant(Instant.ofEpochMilli(now), ZoneId.of("Z"))));
								}
							}).onFailure().recoverWithUni(e -> {
								logger.error("failed to send notification for subscription " + potentialSub.getId(), e);
								long now = System.currentTimeMillis();
								potentialSub.getSubscription().getNotification().setLastFailedNotification(now);
								potentialSub.getSubscription().getNotification().setLastNotification(now);
								return regDAO.updateNotificationFailure(potentialSub.getTenant(), potentialSub.getId(),
										SerializationTools.notifiedAt_formatter.format(
												LocalDateTime.ofInstant(Instant.ofEpochMilli(now), ZoneId.of("Z"))));
							});
				} catch (Exception e) {
					logger.error("failed to send notification for subscription " + potentialSub.getId(), e);
					return Uni.createFrom().voidItem();
				}
				break;
			default:
				logger.error("unsuported endpoint in subscription " + potentialSub.getId());
				return Uni.createFrom().voidItem();
			}
			if (potentialSub.getSubscription().getThrottling() > 0) {
				long delay = potentialSub.getSubscription().getThrottling() - (System.currentTimeMillis()
						- potentialSub.getSubscription().getNotification().getLastNotification());
				if (delay > 0) {
					return Uni.createFrom().voidItem().onItem().delayIt().by(Duration.ofMillis(delay)).onItem()
							.transformToUni(v -> toSend);
				} else {
					return toSend;
				}
			}
			return toSend;
		}
		return Uni.createFrom().voidItem();
	}

	private String getMqttPayload(NotificationParam notificationParam, Map<String, Object> notification)
			throws Exception {
		Map<String, Object> result = Maps.newLinkedHashMap();
		if (!notificationParam.getEndPoint().getReceiverInfo().isEmpty()) {
			result.put(NGSIConstants.METADATA, getMqttMetaData(notificationParam.getEndPoint().getReceiverInfo()));
		}
		result.put(NGSIConstants.BODY, notification);
		return JsonUtils.toString(result);
	}

	private List<Map<String, String>> getMqttMetaData(ArrayListMultimap<String, String> receiverInfo) {
		List<Map<String, String>> result = Lists.newArrayList();
		for (Entry<String, String> entry : receiverInfo.entries()) {
			Map<String, String> tmp = new HashMap<>(1);
			tmp.put(entry.getKey(), entry.getValue());
			result.add(tmp);
		}
		return result;
	}

	private Uni<MqttClient> getMqttClient(NotificationParam notificationParam) {
		URI host = notificationParam.getEndPoint().getUri();
		String hostString = host.getHost() + host.getPort();
		MqttClient client;
		if (!host2MqttClient.containsKey(hostString)) {
			client = MqttClient.create(vertx);
			return client.connect(host.getPort(), host.getHost()).onItem().transform(t -> {
				host2MqttClient.put(hostString, client);
				return client;
			});
		} else {
			client = host2MqttClient.get(hostString);
			if (client.isConnected()) {
				return Uni.createFrom().item(client);
			} else {
				return client.connect(host.getPort(), host.getHost()).onItem().transform(t -> {
					return client;
				});
			}
		}
	}

	private MultiMap getHeaders(NotificationParam notificationParam) {
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

	private Map<String, Object> generateNotification(SubscriptionRequest potentialSub, Object reg, int triggerReason)
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

	@SuppressWarnings("unchecked")
	private boolean shouldSendOut(SubscriptionRequest potentialSub, Map<String, Object> reg) {
		Subscription sub = potentialSub.getSubscription();
		if (!sub.getIsActive() || sub.getExpiresAt() < System.currentTimeMillis()) {
			return false;
		}

		for (EntityInfo entityInfo : sub.getEntities()) {
			if (entityInfo.getId() != null && entityInfo.getType() != null && sub.getAttributeNames() != null) {
				if (checkRegForIdTypeAttrs(entityInfo.getId(), entityInfo.getType(), sub.getAttributeNames(),
						(List<Map<String, Object>>) reg.get(NGSIConstants.NGSI_LD_INFORMATION))) {
					break;
				}
			} else if (entityInfo.getIdPattern() != null && entityInfo.getType() != null
					&& sub.getAttributeNames() != null) {
				if (checkRegForIdPatternTypeAttrs(entityInfo.getIdPattern(), entityInfo.getType(),
						sub.getAttributeNames(),
						(List<Map<String, Object>>) reg.get(NGSIConstants.NGSI_LD_INFORMATION))) {
					break;
				}
			} else if (entityInfo.getId() != null && entityInfo.getType() != null) {
				if (checkRegForIdType(entityInfo.getId(), entityInfo.getType(),
						(List<Map<String, Object>>) reg.get(NGSIConstants.NGSI_LD_INFORMATION))) {
					break;
				}
			} else if (entityInfo.getIdPattern() != null && entityInfo.getType() != null) {
				if (checkRegForIdPatternType(entityInfo.getIdPattern(), entityInfo.getType(),
						(List<Map<String, Object>>) reg.get(NGSIConstants.NGSI_LD_INFORMATION))) {
					break;
				}
			} else if (entityInfo.getId() != null && sub.getAttributeNames() != null) {
				if (checkRegForIdAttrs(entityInfo.getId(), sub.getAttributeNames(),
						(List<Map<String, Object>>) reg.get(NGSIConstants.NGSI_LD_INFORMATION))) {
					break;
				}
			} else if (entityInfo.getIdPattern() != null && sub.getAttributeNames() != null) {
				if (checkRegForIdPatternAttrs(entityInfo.getIdPattern(), sub.getAttributeNames(),
						(List<Map<String, Object>>) reg.get(NGSIConstants.NGSI_LD_INFORMATION))) {
					break;
				}
			} else if (entityInfo.getType() != null && sub.getAttributeNames() != null) {
				if (checkRegForTypeAttrs(entityInfo.getType(), sub.getAttributeNames(),
						(List<Map<String, Object>>) reg.get(NGSIConstants.NGSI_LD_INFORMATION))) {
					break;
				}
			} else if (entityInfo.getType() != null) {
				if (checkRegForType(entityInfo.getType(),
						(List<Map<String, Object>>) reg.get(NGSIConstants.NGSI_LD_INFORMATION))) {
					break;
				}
			} else if (entityInfo.getIdPattern() != null) {
				if (checkRegForIdPattern(entityInfo.getIdPattern(),
						(List<Map<String, Object>>) reg.get(NGSIConstants.NGSI_LD_INFORMATION))) {
					break;
				}
			} else if (entityInfo.getId() != null) {
				if (checkRegForId(entityInfo.getId(),
						(List<Map<String, Object>>) reg.get(NGSIConstants.NGSI_LD_INFORMATION))) {
					break;
				}
			} else if (sub.getAttributeNames() != null) {
				if (checkRegForAttribs(sub.getAttributeNames(),
						(List<Map<String, Object>>) reg.get(NGSIConstants.NGSI_LD_INFORMATION))) {
					break;
				}
			}

		}
		if (!evaluateGeoQuery(sub.getLdGeoQuery(), reg)) {
			return false;
		}
		if (sub.getScopeQuery() != null) {
			if (!sub.getScopeQuery().calculate(EntityTools.getScopes(reg))) {
				return false;
			}
		}
		if (sub.getCsf() != null) {
			if (!sub.getCsf().calculate(EntityTools.getBaseProperties(reg))) {
				return false;
			}
		}
		return true;
	}

	@SuppressWarnings("unchecked")
	private boolean checkRegForId(URI id, List<Map<String, Object>> information) {
		for (Map<String, Object> entry : information) {
			if (!entry.containsKey(NGSIConstants.NGSI_LD_ENTITIES)) {
				return true;
			}
			List<Map<String, Object>> entities = (List<Map<String, Object>>) entry.get(NGSIConstants.NGSI_LD_ENTITIES);
			for (Map<String, Object> entity : entities) {
				if (!entity.containsKey(NGSIConstants.JSON_LD_ID)) {
					return true;
				}
				if (entity.get(NGSIConstants.JSON_LD_ID).equals(id.toString())) {
					return true;
				}
			}
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	private boolean checkRegForIdPattern(String idPattern, List<Map<String, Object>> information) {
		for (Map<String, Object> entry : information) {
			if (!entry.containsKey(NGSIConstants.NGSI_LD_ENTITIES)) {
				return true;
			}
			List<Map<String, Object>> entities = (List<Map<String, Object>>) entry.get(NGSIConstants.NGSI_LD_ENTITIES);
			for (Map<String, Object> entity : entities) {
				if ((!entity.containsKey(NGSIConstants.JSON_LD_ID)
						&& !entity.containsKey(NGSIConstants.NGSI_LD_ID_PATTERN))
						|| (entity.containsKey(NGSIConstants.JSON_LD_ID)
								&& ((String) entity.get(NGSIConstants.JSON_LD_ID)).matches(idPattern))
						|| (entity.containsKey(NGSIConstants.NGSI_LD_ID_PATTERN)
								&& ((List<Map<String, String>>) entity.get(NGSIConstants.NGSI_LD_ID_PATTERN)).get(0)
										.get(NGSIConstants.JSON_LD_VALUE).equals(idPattern))) {
					return true;
				}
			}
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	private boolean checkRegForAttribs(Set<String> attributeNames, List<Map<String, Object>> information) {
		for (Map<String, Object> entry : information) {
			if (!entry.containsKey(NGSIConstants.NGSI_LD_RELATIONSHIPS)
					&& !entry.containsKey(NGSIConstants.NGSI_LD_PROPERTIES)) {
				return true;
			}
			if (entry.containsKey(NGSIConstants.NGSI_LD_RELATIONSHIPS)) {
				List<Map<String, String>> relationships = (List<Map<String, String>>) entry
						.get(NGSIConstants.NGSI_LD_RELATIONSHIPS);
				for (Map<String, String> relationship : relationships) {
					if (attributeNames.contains(relationship.get(NGSIConstants.JSON_LD_VALUE))) {
						return true;
					}
				}
			}
			if (entry.containsKey(NGSIConstants.NGSI_LD_PROPERTIES)) {
				List<Map<String, String>> properties = (List<Map<String, String>>) entry
						.get(NGSIConstants.NGSI_LD_PROPERTIES);
				for (Map<String, String> property : properties) {
					if (attributeNames.contains(property.get(NGSIConstants.JSON_LD_VALUE))) {
						return true;
					}
				}
			}
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	private boolean checkRegForType(String type, List<Map<String, Object>> information) {
		for (Map<String, Object> entry : information) {
			if (!entry.containsKey(NGSIConstants.NGSI_LD_ENTITIES)) {
				return true;
			}
			List<Map<String, Object>> entities = (List<Map<String, Object>>) entry.get(NGSIConstants.NGSI_LD_ENTITIES);
			for (Map<String, Object> entity : entities) {
				if (!entity.containsKey(NGSIConstants.JSON_LD_TYPE)
						|| ((List<String>) entity.get(NGSIConstants.JSON_LD_TYPE)).contains(type)) {
					return true;
				}
			}
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	private boolean checkRegForTypeAttrs(String type, Set<String> attributeNames,
			List<Map<String, Object>> information) {
		for (Map<String, Object> entry : information) {
			if (!entry.containsKey(NGSIConstants.NGSI_LD_ENTITIES)) {
				if (entry.containsKey(NGSIConstants.NGSI_LD_RELATIONSHIPS)) {
					List<Map<String, String>> relationships = (List<Map<String, String>>) entry
							.get(NGSIConstants.NGSI_LD_RELATIONSHIPS);
					for (Map<String, String> relationship : relationships) {
						if (attributeNames.contains(relationship.get(NGSIConstants.JSON_LD_VALUE))) {
							return true;
						}
					}
				}
				if (entry.containsKey(NGSIConstants.NGSI_LD_PROPERTIES)) {
					List<Map<String, String>> properties = (List<Map<String, String>>) entry
							.get(NGSIConstants.NGSI_LD_PROPERTIES);
					for (Map<String, String> property : properties) {
						if (attributeNames.contains(property.get(NGSIConstants.JSON_LD_VALUE))) {
							return true;
						}
					}
				}
			} else if (!entry.containsKey(NGSIConstants.NGSI_LD_RELATIONSHIPS)
					&& !entry.containsKey(NGSIConstants.NGSI_LD_PROPERTIES)) {
				List<Map<String, Object>> entities = (List<Map<String, Object>>) entry
						.get(NGSIConstants.NGSI_LD_ENTITIES);
				for (Map<String, Object> entity : entities) {
					if (!entity.containsKey(NGSIConstants.JSON_LD_TYPE)
							|| ((List<String>) entity.get(NGSIConstants.JSON_LD_TYPE)).contains(type)) {
						return true;
					}
				}
			} else {
				List<Map<String, Object>> entities = (List<Map<String, Object>>) entry
						.get(NGSIConstants.NGSI_LD_ENTITIES);
				boolean typeFound = false;
				for (Map<String, Object> entity : entities) {
					if (!entity.containsKey(NGSIConstants.JSON_LD_TYPE)
							|| ((List<String>) entity.get(NGSIConstants.JSON_LD_TYPE)).contains(type)) {
						typeFound = true;
						break;
					}
				}
				if (!typeFound) {
					return false;
				}
				if (entry.containsKey(NGSIConstants.NGSI_LD_RELATIONSHIPS)) {
					List<Map<String, String>> relationships = (List<Map<String, String>>) entry
							.get(NGSIConstants.NGSI_LD_RELATIONSHIPS);
					for (Map<String, String> relationship : relationships) {
						if (attributeNames.contains(relationship.get(NGSIConstants.JSON_LD_VALUE))) {
							return true;
						}
					}
				}
				if (entry.containsKey(NGSIConstants.NGSI_LD_PROPERTIES)) {
					List<Map<String, String>> properties = (List<Map<String, String>>) entry
							.get(NGSIConstants.NGSI_LD_PROPERTIES);
					for (Map<String, String> property : properties) {
						if (attributeNames.contains(property.get(NGSIConstants.JSON_LD_VALUE))) {
							return true;
						}
					}
				}
			}
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	private boolean checkRegForIdPatternAttrs(String idPattern, Set<String> attributeNames,
			List<Map<String, Object>> information) {
		for (Map<String, Object> entry : information) {
			if (!entry.containsKey(NGSIConstants.NGSI_LD_ENTITIES)) {
				if (entry.containsKey(NGSIConstants.NGSI_LD_RELATIONSHIPS)) {
					List<Map<String, String>> relationships = (List<Map<String, String>>) entry
							.get(NGSIConstants.NGSI_LD_RELATIONSHIPS);
					for (Map<String, String> relationship : relationships) {
						if (attributeNames.contains(relationship.get(NGSIConstants.JSON_LD_VALUE))) {
							return true;
						}
					}
				}
				if (entry.containsKey(NGSIConstants.NGSI_LD_PROPERTIES)) {
					List<Map<String, String>> properties = (List<Map<String, String>>) entry
							.get(NGSIConstants.NGSI_LD_PROPERTIES);
					for (Map<String, String> property : properties) {
						if (attributeNames.contains(property.get(NGSIConstants.JSON_LD_VALUE))) {
							return true;
						}
					}
				}
			} else if (!entry.containsKey(NGSIConstants.NGSI_LD_RELATIONSHIPS)
					&& !entry.containsKey(NGSIConstants.NGSI_LD_PROPERTIES)) {
				List<Map<String, Object>> entities = (List<Map<String, Object>>) entry
						.get(NGSIConstants.NGSI_LD_ENTITIES);
				for (Map<String, Object> entity : entities) {
					if (!entity.containsKey(NGSIConstants.JSON_LD_ID)
							&& !entity.containsKey(NGSIConstants.NGSI_LD_ID_PATTERN)) {
						return true;
					}
					if (entity.containsKey(NGSIConstants.JSON_LD_ID)
							&& ((String) entity.get(NGSIConstants.JSON_LD_ID)).matches(idPattern)) {
						return true;
					}
					if (entity.containsKey(NGSIConstants.NGSI_LD_ID_PATTERN)
							&& ((List<Map<String, String>>) entity.get(NGSIConstants.NGSI_LD_ID_PATTERN)).get(0)
									.get(NGSIConstants.JSON_LD_VALUE).equals(idPattern)) {
						return true;
					}
				}
			} else {

				boolean idPatternFound = false;
				List<Map<String, Object>> entities = (List<Map<String, Object>>) entry
						.get(NGSIConstants.NGSI_LD_ENTITIES);
				for (Map<String, Object> entity : entities) {
					if (!entity.containsKey(NGSIConstants.JSON_LD_ID)
							&& !entity.containsKey(NGSIConstants.NGSI_LD_ID_PATTERN)) {
						idPatternFound = true;
						break;
					}
					if (entity.containsKey(NGSIConstants.JSON_LD_ID)
							&& ((String) entity.get(NGSIConstants.JSON_LD_ID)).matches(idPattern)) {
						idPatternFound = true;
						break;
					}
					if (entity.containsKey(NGSIConstants.NGSI_LD_ID_PATTERN)
							&& ((List<Map<String, String>>) entity.get(NGSIConstants.NGSI_LD_ID_PATTERN)).get(0)
									.get(NGSIConstants.JSON_LD_VALUE).equals(idPattern)) {
						idPatternFound = true;
						break;
					}
				}
				if (!idPatternFound) {
					return false;
				}
				if (entry.containsKey(NGSIConstants.NGSI_LD_RELATIONSHIPS)) {
					List<Map<String, String>> relationships = (List<Map<String, String>>) entry
							.get(NGSIConstants.NGSI_LD_RELATIONSHIPS);
					for (Map<String, String> relationship : relationships) {
						if (attributeNames.contains(relationship.get(NGSIConstants.JSON_LD_VALUE))) {
							return true;
						}
					}
				}
				if (entry.containsKey(NGSIConstants.NGSI_LD_PROPERTIES)) {
					List<Map<String, String>> properties = (List<Map<String, String>>) entry
							.get(NGSIConstants.NGSI_LD_PROPERTIES);
					for (Map<String, String> property : properties) {
						if (attributeNames.contains(property.get(NGSIConstants.JSON_LD_VALUE))) {
							return true;
						}
					}
				}
			}
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	private boolean checkRegForIdAttrs(URI id, Set<String> attributeNames, List<Map<String, Object>> information) {
		for (Map<String, Object> entry : information) {
			if (!entry.containsKey(NGSIConstants.NGSI_LD_ENTITIES)) {
				if (entry.containsKey(NGSIConstants.NGSI_LD_RELATIONSHIPS)) {
					List<Map<String, String>> relationships = (List<Map<String, String>>) entry
							.get(NGSIConstants.NGSI_LD_RELATIONSHIPS);
					for (Map<String, String> relationship : relationships) {
						if (attributeNames.contains(relationship.get(NGSIConstants.JSON_LD_VALUE))) {
							return true;
						}
					}
				}
				if (entry.containsKey(NGSIConstants.NGSI_LD_PROPERTIES)) {
					List<Map<String, String>> properties = (List<Map<String, String>>) entry
							.get(NGSIConstants.NGSI_LD_PROPERTIES);
					for (Map<String, String> property : properties) {
						if (attributeNames.contains(property.get(NGSIConstants.JSON_LD_VALUE))) {
							return true;
						}
					}
				}
			} else if (!entry.containsKey(NGSIConstants.NGSI_LD_RELATIONSHIPS)
					&& !entry.containsKey(NGSIConstants.NGSI_LD_PROPERTIES)) {
				List<Map<String, Object>> entities = (List<Map<String, Object>>) entry
						.get(NGSIConstants.NGSI_LD_ENTITIES);
				for (Map<String, Object> entity : entities) {
					if (!entity.containsKey(NGSIConstants.JSON_LD_ID)) {
						return true;
					}
					if (entity.get(NGSIConstants.JSON_LD_ID).equals(id.toString())) {
						return true;
					}
				}
			} else {

				List<Map<String, Object>> entities = (List<Map<String, Object>>) entry
						.get(NGSIConstants.NGSI_LD_ENTITIES);
				boolean idFound = false;

				for (Map<String, Object> entity : entities) {
					if (!entity.containsKey(NGSIConstants.JSON_LD_ID)) {
						idFound = true;
						break;
					}
					if (entity.get(NGSIConstants.JSON_LD_ID).equals(id.toString())) {
						idFound = true;
						break;
					}
				}
				if (!idFound) {
					return false;
				}
				if (entry.containsKey(NGSIConstants.NGSI_LD_RELATIONSHIPS)) {
					List<Map<String, String>> relationships = (List<Map<String, String>>) entry
							.get(NGSIConstants.NGSI_LD_RELATIONSHIPS);
					for (Map<String, String> relationship : relationships) {
						if (attributeNames.contains(relationship.get(NGSIConstants.JSON_LD_VALUE))) {
							return true;
						}
					}
				}
				if (entry.containsKey(NGSIConstants.NGSI_LD_PROPERTIES)) {
					List<Map<String, String>> properties = (List<Map<String, String>>) entry
							.get(NGSIConstants.NGSI_LD_PROPERTIES);
					for (Map<String, String> property : properties) {
						if (attributeNames.contains(property.get(NGSIConstants.JSON_LD_VALUE))) {
							return true;
						}
					}
				}
			}
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	private boolean checkRegForIdPatternType(String idPattern, String type, List<Map<String, Object>> information) {
		for (Map<String, Object> entry : information) {
			if (!entry.containsKey(NGSIConstants.NGSI_LD_ENTITIES)) {
				return true;
			}
			List<Map<String, Object>> entities = (List<Map<String, Object>>) entry.get(NGSIConstants.NGSI_LD_ENTITIES);
			for (Map<String, Object> entity : entities) {
				if (((!entity.containsKey(NGSIConstants.JSON_LD_ID)
						&& !entity.containsKey(NGSIConstants.NGSI_LD_ID_PATTERN))
						|| (entity.containsKey(NGSIConstants.JSON_LD_ID)
								&& ((String) entity.get(NGSIConstants.JSON_LD_ID)).matches(idPattern))
						|| (entity.containsKey(NGSIConstants.NGSI_LD_ID_PATTERN)
								&& ((List<Map<String, String>>) entity.get(NGSIConstants.NGSI_LD_ID_PATTERN)).get(0)
										.get(NGSIConstants.JSON_LD_VALUE).equals(idPattern)))
						&& (!entity.containsKey(NGSIConstants.JSON_LD_TYPE)
								|| ((List<String>) entity.get(NGSIConstants.JSON_LD_TYPE)).contains(type))) {
					return true;
				}
			}
		}
		return false;

	}

	@SuppressWarnings("unchecked")
	private boolean checkRegForIdType(URI id, String type, List<Map<String, Object>> information) {
		for (Map<String, Object> entry : information) {
			if (!entry.containsKey(NGSIConstants.NGSI_LD_ENTITIES)) {
				return true;
			}
			List<Map<String, Object>> entities = (List<Map<String, Object>>) entry.get(NGSIConstants.NGSI_LD_ENTITIES);
			for (Map<String, Object> entity : entities) {
				if ((!entity.containsKey(NGSIConstants.JSON_LD_ID)
						|| entity.get(NGSIConstants.JSON_LD_ID).equals(id.toString()))
						&& (!entity.containsKey(NGSIConstants.JSON_LD_TYPE)
								|| ((List<String>) entity.get(NGSIConstants.JSON_LD_TYPE)).contains(type))) {
					return true;
				}
			}
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	private boolean checkRegForIdPatternTypeAttrs(String idPattern, String type, Set<String> attributeNames,
			List<Map<String, Object>> information) {
		for (Map<String, Object> entry : information) {
			if (!entry.containsKey(NGSIConstants.NGSI_LD_ENTITIES)) {
				if (entry.containsKey(NGSIConstants.NGSI_LD_RELATIONSHIPS)) {
					List<Map<String, String>> relationships = (List<Map<String, String>>) entry
							.get(NGSIConstants.NGSI_LD_RELATIONSHIPS);
					for (Map<String, String> relationship : relationships) {
						if (attributeNames.contains(relationship.get(NGSIConstants.JSON_LD_VALUE))) {
							return true;
						}
					}
				}
				if (entry.containsKey(NGSIConstants.NGSI_LD_PROPERTIES)) {
					List<Map<String, String>> properties = (List<Map<String, String>>) entry
							.get(NGSIConstants.NGSI_LD_PROPERTIES);
					for (Map<String, String> property : properties) {
						if (attributeNames.contains(property.get(NGSIConstants.JSON_LD_VALUE))) {
							return true;
						}
					}
				}
			} else if (!entry.containsKey(NGSIConstants.NGSI_LD_RELATIONSHIPS)
					&& !entry.containsKey(NGSIConstants.NGSI_LD_PROPERTIES)) {
				List<Map<String, Object>> entities = (List<Map<String, Object>>) entry
						.get(NGSIConstants.NGSI_LD_ENTITIES);
				for (Map<String, Object> entity : entities) {
					if (((!entity.containsKey(NGSIConstants.JSON_LD_ID)
							&& !entity.containsKey(NGSIConstants.NGSI_LD_ID_PATTERN))
							|| (entity.containsKey(NGSIConstants.JSON_LD_ID)
									&& ((String) entity.get(NGSIConstants.JSON_LD_ID)).matches(idPattern))
							|| (entity.containsKey(NGSIConstants.NGSI_LD_ID_PATTERN)
									&& ((List<Map<String, String>>) entity.get(NGSIConstants.NGSI_LD_ID_PATTERN)).get(0)
											.get(NGSIConstants.JSON_LD_VALUE).equals(idPattern)))
							&& (!entity.containsKey(NGSIConstants.JSON_LD_TYPE)
									|| ((List<String>) entity.get(NGSIConstants.JSON_LD_TYPE)).contains(type))) {
						return true;
					}
				}
			} else {
				List<Map<String, Object>> entities = (List<Map<String, Object>>) entry
						.get(NGSIConstants.NGSI_LD_ENTITIES);
				boolean idPatternAndTypeFound = false;
				for (Map<String, Object> entity : entities) {
					if (((!entity.containsKey(NGSIConstants.JSON_LD_ID)
							&& !entity.containsKey(NGSIConstants.NGSI_LD_ID_PATTERN))
							|| (entity.containsKey(NGSIConstants.JSON_LD_ID)
									&& ((String) entity.get(NGSIConstants.JSON_LD_ID)).matches(idPattern))
							|| (entity.containsKey(NGSIConstants.NGSI_LD_ID_PATTERN)
									&& ((List<Map<String, String>>) entity.get(NGSIConstants.NGSI_LD_ID_PATTERN)).get(0)
											.get(NGSIConstants.JSON_LD_VALUE).equals(idPattern)))
							&& (!entity.containsKey(NGSIConstants.JSON_LD_TYPE)
									|| ((List<String>) entity.get(NGSIConstants.JSON_LD_TYPE)).contains(type))) {
						idPatternAndTypeFound = true;
						break;
					}
				}

				if (!idPatternAndTypeFound) {
					return false;
				}
				if (entry.containsKey(NGSIConstants.NGSI_LD_RELATIONSHIPS)) {
					List<Map<String, String>> relationships = (List<Map<String, String>>) entry
							.get(NGSIConstants.NGSI_LD_RELATIONSHIPS);
					for (Map<String, String> relationship : relationships) {
						if (attributeNames.contains(relationship.get(NGSIConstants.JSON_LD_VALUE))) {
							return true;
						}
					}
				}
				if (entry.containsKey(NGSIConstants.NGSI_LD_PROPERTIES)) {
					List<Map<String, String>> properties = (List<Map<String, String>>) entry
							.get(NGSIConstants.NGSI_LD_PROPERTIES);
					for (Map<String, String> property : properties) {
						if (attributeNames.contains(property.get(NGSIConstants.JSON_LD_VALUE))) {
							return true;
						}
					}
				}
			}
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	private boolean checkRegForIdTypeAttrs(URI id, String type, Set<String> attributeNames,
			List<Map<String, Object>> information) {
		for (Map<String, Object> entry : information) {
			if (!entry.containsKey(NGSIConstants.NGSI_LD_ENTITIES)) {
				if (entry.containsKey(NGSIConstants.NGSI_LD_RELATIONSHIPS)) {
					List<Map<String, String>> relationships = (List<Map<String, String>>) entry
							.get(NGSIConstants.NGSI_LD_RELATIONSHIPS);
					for (Map<String, String> relationship : relationships) {
						if (attributeNames.contains(relationship.get(NGSIConstants.JSON_LD_VALUE))) {
							return true;
						}
					}
				}
				if (entry.containsKey(NGSIConstants.NGSI_LD_PROPERTIES)) {
					List<Map<String, String>> properties = (List<Map<String, String>>) entry
							.get(NGSIConstants.NGSI_LD_PROPERTIES);
					for (Map<String, String> property : properties) {
						if (attributeNames.contains(property.get(NGSIConstants.JSON_LD_VALUE))) {
							return true;
						}
					}
				}
			} else if (!entry.containsKey(NGSIConstants.NGSI_LD_RELATIONSHIPS)
					&& !entry.containsKey(NGSIConstants.NGSI_LD_PROPERTIES)) {
				List<Map<String, Object>> entities = (List<Map<String, Object>>) entry
						.get(NGSIConstants.NGSI_LD_ENTITIES);
				for (Map<String, Object> entity : entities) {
					if ((!entity.containsKey(NGSIConstants.JSON_LD_ID)
							|| entity.get(NGSIConstants.JSON_LD_ID).equals(id.toString()))
							&& (!entity.containsKey(NGSIConstants.JSON_LD_TYPE)
									|| ((List<String>) entity.get(NGSIConstants.JSON_LD_TYPE)).contains(type))) {
						return true;
					}
				}
			} else {
				List<Map<String, Object>> entities = (List<Map<String, Object>>) entry
						.get(NGSIConstants.NGSI_LD_ENTITIES);
				boolean idAndTypeFound = false;
				for (Map<String, Object> entity : entities) {
					if ((!entity.containsKey(NGSIConstants.JSON_LD_ID)
							|| entity.get(NGSIConstants.JSON_LD_ID).equals(id.toString()))
							&& (!entity.containsKey(NGSIConstants.JSON_LD_TYPE)
									|| ((List<String>) entity.get(NGSIConstants.JSON_LD_TYPE)).contains(type))) {
						idAndTypeFound = true;
						break;
					}
				}

				if (!idAndTypeFound) {
					return false;
				}
				if (entry.containsKey(NGSIConstants.NGSI_LD_RELATIONSHIPS)) {
					List<Map<String, String>> relationships = (List<Map<String, String>>) entry
							.get(NGSIConstants.NGSI_LD_RELATIONSHIPS);
					for (Map<String, String> relationship : relationships) {
						if (attributeNames.contains(relationship.get(NGSIConstants.JSON_LD_VALUE))) {
							return true;
						}
					}
				}
				if (entry.containsKey(NGSIConstants.NGSI_LD_PROPERTIES)) {
					List<Map<String, String>> properties = (List<Map<String, String>>) entry
							.get(NGSIConstants.NGSI_LD_PROPERTIES);
					for (Map<String, String> property : properties) {
						if (attributeNames.contains(property.get(NGSIConstants.JSON_LD_VALUE))) {
							return true;
						}
					}
				}
			}
		}
		return false;
	}

	public Uni<Void> handleInternalSubscription(SubscriptionRequest message) {
		if (message.getRequestType() == AppConstants.DELETE_SUBSCRIPTION_REQUEST) {
			tenant2subscriptionId2Subscription.remove(message.getTenant(), message.getId());
		}
		try {
			message.setSubscription(Subscription.expandSubscription(message.getPayload(), message.getContext(), false));
		} catch (ResponseException e) {
			logger.error("Failed to load internal subscription", e);
		}
		boolean sendNotification = !tenant2subscriptionId2Subscription.contains(message.getTenant(), message.getId());
		try {
			message.getSubscription().getNotification().getEndPoint().setUri(new URI("internal:kafka"));
			// if there is attribs and there is q take attribs from q as well into attrs
			if (message.getSubscription().getAttributeNames() != null
					&& !message.getSubscription().getAttributeNames().isEmpty()
					&& message.getSubscription().getLdQuery() != null) {
				message.getSubscription().getAttributeNames()
						.addAll(message.getSubscription().getLdQuery().getAllAttibs());
			}
			message.getSubscription().setThrottling(0);
			message.getSubscription().setTimeInterval(0);
			tenant2subscriptionId2Subscription.put(message.getTenant(), message.getId(), message);
		} catch (URISyntaxException e) {
			// left empty intentionally this will never throw because it's a constant string
			// we control
		}
		if (sendNotification) {
			return regDAO.getInitialNotificationData(message).onItem().transformToUni(rows -> {
				List<Map<String, Object>> data = Lists.newArrayList();
				rows.forEach(row -> {
					data.add(row.getJsonObject(0).getMap());
				});
				try {
					return internalNotificationSender
							.send(new InternalNotification(message.getTenant(), message.getId(),
									generateNotification(message, data, AppConstants.INTERNAL_NOTIFICATION_REQUEST)));
				} catch (Exception e) {
					logger.error("Failed to send internal notification for sub " + message.getId(), e);
					return Uni.createFrom().voidItem();
				}
			});
		} else {
			return Uni.createFrom().voidItem();
		}
	}

	protected boolean shouldFire(Map<String, Object> entry, SubscriptionRequest subscription) {
		Set<String> attribs = subscription.getSubscription().getAttributeNames();

		if (attribs == null || attribs.isEmpty()) {
			return true;
		}
		if (entry.containsKey(NGSIConstants.NGSI_LD_INFORMATION)) {
			@SuppressWarnings("unchecked")
			List<Map<String, Object>> information = (List<Map<String, Object>>) entry
					.get(NGSIConstants.NGSI_LD_INFORMATION);
			for (Map<String, Object> informationEntry : information) {
				Object propertyNames = informationEntry.get(NGSIConstants.NGSI_LD_PROPERTIES);
				Object relationshipNames = informationEntry.get(NGSIConstants.NGSI_LD_RELATIONSHIPS);
				if (relationshipNames == null && relationshipNames == null) {
					return true;
				}
				if (relationshipNames != null) {
					@SuppressWarnings("unchecked")
					List<Map<String, String>> list = (List<Map<String, String>>) relationshipNames;
					for (Map<String, String> relationshipEntry : list) {
						if (attribs.contains(relationshipEntry.get(NGSIConstants.JSON_LD_ID))) {
							return true;
						}
					}
				}
				if (propertyNames != null) {
					@SuppressWarnings("unchecked")
					List<Map<String, String>> list = (List<Map<String, String>>) propertyNames;
					for (Map<String, String> propertyEntry : list) {
						if (attribs.contains(propertyEntry.get(NGSIConstants.JSON_LD_ID))) {
							return true;
						}
					}
				}
			}
		}
		// TODO add aditional changes on what could fire in a csource reg
		return false;
	}

	public static void main(String[] args) throws URISyntaxException {
		URI uri1 = new URI("http://test1.com");
		URI uri2 = new URI("http://test1.com/");

		System.out.println(uri1.toString());
		System.out.println(uri2.toString());
	}

	@Scheduled(every = "${scorpio.registry.subscription.checkinterval}")
	Uni<Void> checkIntervalSubs() {
		List<Uni<Void>> unis = Lists.newArrayList();
		for (Cell<String, String, SubscriptionRequest> cell : tenant2subscriptionId2IntervalSubscription.cellSet()) {
			SubscriptionRequest request = cell.getValue();
			Subscription sub = request.getSubscription();
			long now = System.currentTimeMillis();
			if (sub.getNotification().getLastNotification() + sub.getTimeInterval() < now) {
				unis.add(regDAO.getInitialNotificationData(request).onItem().transformToUni(rows -> {
					if (rows.size() == 0) {
						return Uni.createFrom().voidItem();
					}
					List<Map<String, Object>> data = Lists.newArrayList();
					rows.forEach(row -> {
						data.add(row.getJsonObject(0).getMap());
					});
					try {
						return sendNotification(request,
								generateNotification(request, data, AppConstants.INTERNAL_NOTIFICATION_REQUEST),
								AppConstants.INTERVAL_NOTIFICATION_REQUEST);
					} catch (Exception e) {
						logger.error("Failed to send initial notifcation", e);
						return Uni.createFrom().voidItem();
					}

				}));
			}
		}
		return Uni.combine().all().unis(unis).discardItems();
	}

	@SuppressWarnings("unchecked")
	private boolean evaluateGeoQuery(GeoQueryTerm geoQuery, Map<String, Object> location) {
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

}
