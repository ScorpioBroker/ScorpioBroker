package eu.neclab.ngsildbroker.registry.subscriptionmanager.service;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.quartz.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;

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
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;
import eu.neclab.ngsildbroker.registry.subscriptionmanager.repository.RegistrySubscriptionInfoDAO;
import io.netty.handler.codec.mqtt.MqttQoS;
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

	@Inject
	Scheduler scheduler;

	protected Table<String, String, SubscriptionRequest> tenant2subscriptionId2Subscription = HashBasedTable.create();

	private WebClient webClient;

	private Object ALL_TYPES_SUB;

	@PostConstruct
	Uni<Void> setup() {
		this.webClient = WebClient.create(vertx);

		return regDAO.loadSubscriptions().onItem().transformToUni(subs -> {
			subs.forEach(tuple -> {
				SubscriptionRequest request;
				try {
					request = new SubscriptionRequest(tuple.getItem1(), tuple.getItem2(),
							new Context().parse(tuple.getItem3(), false));
					this.tenant2subscriptionId2Subscription.put(request.getTenant(), request.getId(), request);
				} catch (Exception e) {
					logger.error("Failed to load stored subscription " + tuple.getItem1());
				}
			});
			return Uni.createFrom().voidItem();
		});

	}

	public Uni<NGSILDOperationResult> createSubscription(String tenant, Map<String, Object> subscription,
			Context context) {
		SubscriptionRequest request;
		try {
			request = new SubscriptionRequest(tenant, subscription, context);
		} catch (ResponseException e) {
			return Uni.createFrom().failure(e);
		}
		return regDAO.createSubscription(request).onItem().transform(t -> {
			tenant2subscriptionId2Subscription.put(tenant, request.getId(), request);
			return new NGSILDOperationResult(AppConstants.CREATE_SUBSCRIPTION_REQUEST, request.getId());
		}).onFailure().recoverWithUni(e -> {
			// TODO sql check
			return Uni.createFrom().failure(new ResponseException(ErrorType.AlreadyExists,
					"Subscription with id " + request.getId() + " exists"));
		});
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
			tenant2subscriptionId2Subscription.put(tenant, request.getId(), updatedRequest);
			return Uni.createFrom()
					.item(new NGSILDOperationResult(AppConstants.UPDATE_SUBSCRIPTION_REQUEST, subscriptionId));
		});
	}

	public Uni<NGSILDOperationResult> deleteSubscription(String tenant, String subscriptionId) {
		DeleteSubscriptionRequest request = new DeleteSubscriptionRequest(tenant, subscriptionId);
		return regDAO.deleteSubscription(request).onItem().transform(t -> {
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
		return Uni.combine().all().unis(unis).combinedWith(null);
	}

	private Uni<Void> sendNotification(SubscriptionRequest potentialSub, Map<String, Object> reg, int triggerReason) {
		if (shouldSendOut(potentialSub, reg)) {
			Map<String, Object> notification;
			try {
				notification = generateNotification(potentialSub, reg, triggerReason);
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
				return Uni.createFrom().voidItem();
			}
			NotificationParam notificationParam = potentialSub.getSubscription().getNotification();

			switch (notificationParam.getEndPoint().getUri().getScheme()) {
			case "internal":
				return internalNotificationSender
						.send(new InternalNotification(potentialSub.getTenant(), potentialSub.getId(), notification));
			case "mqtt":
			case "mqtts":
				try {
					return getMqttClient(notificationParam).onItem().transformToUni(client -> {
						int qos = 1;

						String qosString = notificationParam.getEndPoint().getNotifierInfo()
								.get(NGSIConstants.MQTT_QOS);
						if (qosString != null) {
							qos = Integer.parseInt(qosString);
						}
						return client.publish(notificationParam.getEndPoint().getUri().getPath().substring(1),
								Buffer.buffer(getMqttPayload(notificationParam, notification)), MqttQoS.valueOf(qos),
								false, false).onItem().transformToUni(t -> {
									if (t == 0) {
										// TODO what the fuck is the result here
									}
									return regDAO.updateNotificationSuccess(potentialSub.getTenant(),
											potentialSub.getId(),
											SerializationTools.notifiedAt_formatter.format(LocalDateTime.ofInstant(
													Instant.ofEpochMilli(System.currentTimeMillis()), ZoneId.of("Z"))));
								}).onFailure().recoverWithUni(e -> {
									logger.error("failed to send notification for subscription " + potentialSub.getId(),
											e);
									return regDAO.updateNotificationFailure(potentialSub.getTenant(),
											potentialSub.getId(),
											SerializationTools.notifiedAt_formatter.format(LocalDateTime.ofInstant(
													Instant.ofEpochMilli(System.currentTimeMillis()), ZoneId.of("Z"))));
								});
					});
				} catch (Exception e) {
					logger.error("failed to send notification for subscription " + potentialSub.getId(), e);
					return Uni.createFrom().voidItem();
				}

			case "http":
			case "https":
				try {
					return webClient.post(notificationParam.getEndPoint().getUri().toString())
							.putHeaders(getHeaders(notificationParam))
							.sendBuffer(Buffer.buffer(JsonUtils.toPrettyString(JsonLdProcessor.compact(notification,
									null, potentialSub.getContext(), HttpUtils.opts, -1))))
							.onFailure().retry().atMost(3).onItem().transformToUni(result -> {
								int statusCode = result.statusCode();

								return regDAO
										.updateNotificationSuccess(potentialSub.getTenant(), potentialSub.getId(),
												SerializationTools.notifiedAt_formatter.format(LocalDateTime.ofInstant(
														Instant.ofEpochMilli(System.currentTimeMillis()),
														ZoneId.of("Z"))));
							}).onFailure().recoverWithUni(e -> {
								logger.error("failed to send notification for subscription " + potentialSub.getId(), e);
								return regDAO
										.updateNotificationFailure(potentialSub.getTenant(), potentialSub.getId(),
												SerializationTools.notifiedAt_formatter.format(LocalDateTime.ofInstant(
														Instant.ofEpochMilli(System.currentTimeMillis()),
														ZoneId.of("Z"))));
							});
				} catch (Exception e) {
					logger.error("failed to send notification for subscription " + potentialSub.getId(), e);
					return Uni.createFrom().voidItem();
				}

			default:
				logger.error("unsuported endpoint in subscription " + potentialSub.getId());
				return Uni.createFrom().voidItem();
			}

		}
		return Uni.createFrom().voidItem();
	}

	private String getMqttPayload(NotificationParam notificationParam, Map<String, Object> notification) {
		// TODO Auto-generated method stub
		return null;
	}

	private Uni<MqttClient> getMqttClient(NotificationParam notificationParam) {
		
		return null;
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
		result.add("accept", accept);
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
		notification.put(NGSIConstants.NGSI_LD_DATA_SHORT,
				Lists.newArrayList(JsonLdProcessor.compact(reg, null, potentialSub.getContext(), HttpUtils.opts, -1)));
		notification.put(NGSIConstants.NGSI_LD_TRIGGER_REASON_SHORT, HttpUtils.getTriggerReason(triggerReason));
		return notification;
	}

	private boolean shouldSendOut(SubscriptionRequest potentialSub, Map<String, Object> reg) {
		Subscription sub = potentialSub.getSubscription();
		if (!sub.getIsActive() || sub.getExpiresAt() < System.currentTimeMillis()) {
			return false;
		}
		for (EntityInfo entityInfo : sub.getEntities()) {
			if (entityInfo.getType().equals(ALL_TYPES_SUB)) {
				continue;
			}
			if (entityInfo.getId() == null && entityInfo.getIdPattern() == null) {
				continue;
			}
			if (entityInfo.getId() != null && entityInfo.getId().toString().equals(id)) {
				continue;
			}
			if (entityInfo.getIdPattern() != null && id.matches(entityInfo.getIdPattern())) {
				continue;
			}
		}
		return false;
	}

	public Uni<Void> handleInternalSubscription(SubscriptionRequest message) {
		try {
			message.setSubscription(Subscription.expandSubscription(message.getPayload(), message.getContext(), false));
		} catch (ResponseException e) {
			logger.error("Failed to load internal subscription", e);
		}
		try {
			message.getSubscription().getNotification().getEndPoint().setUri(new URI("internal:kafka"));
			// if there is attribs and there is q take attribs from q as well into attrs
			if (message.getSubscription().getAttributeNames() != null
					&& !message.getSubscription().getAttributeNames().isEmpty()
					&& message.getSubscription().getLdQuery() != null) {
				message.getSubscription().getAttributeNames()
						.addAll(message.getSubscription().getLdQuery().getAllAttibs());
			}
			tenant2subscriptionId2Subscription.put(message.getTenant(), message.getId(), message);
		} catch (URISyntaxException e) {
			// left empty intentionally this will never throw because it's a constant string
			// we control
		}
		return regDAO.getInitialNotificationData(message).onItem().transformToUni(rows -> {
			List<Map<String, Object>> data = Lists.newArrayList();
			rows.forEach(row -> {
				data.add(row.getJsonObject(0).getMap());
			});
			try {
				return internalNotificationSender.send(new InternalNotification(message.getTenant(), message.getId(),
						generateNotification(message, data, AppConstants.INTERNAL_NOTIFICATION_REQUEST)));
			} catch (Exception e) {
				logger.error("Failed to send internal notification for sub " + message.getId(), e);
				return Uni.createFrom().voidItem();
			}
		});
	}

	protected boolean shouldFire(Map<String, Object> entry, SubscriptionRequest subscription) {
		Set<String> attribs = subscription.getSubscription().getAttributeNames();
		if (attribs == null || attribs.isEmpty()) {
			return true;
		}

		List<Map<String, Object>> information = (List<Map<String, Object>>) entry
				.get(NGSIConstants.NGSI_LD_INFORMATION);
		for (Map<String, Object> informationEntry : information) {
			Object propertyNames = informationEntry.get(NGSIConstants.NGSI_LD_PROPERTIES);
			Object relationshipNames = informationEntry.get(NGSIConstants.NGSI_LD_RELATIONSHIPS);
			if (relationshipNames == null && relationshipNames == null) {
				return true;
			}
			if (relationshipNames != null) {
				List<Map<String, String>> list = (List<Map<String, String>>) relationshipNames;
				for (Map<String, String> relationshipEntry : list) {
					if (attribs.contains(relationshipEntry.get(NGSIConstants.JSON_LD_ID))) {
						return true;
					}
				}
			}
			if (propertyNames != null) {
				List<Map<String, String>> list = (List<Map<String, String>>) propertyNames;
				for (Map<String, String> propertyEntry : list) {
					if (attribs.contains(propertyEntry.get(NGSIConstants.JSON_LD_ID))) {
						return true;
					}
				}
			}
		}
		return false;
	}

}
