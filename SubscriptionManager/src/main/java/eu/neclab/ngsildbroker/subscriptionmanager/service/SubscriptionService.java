package eu.neclab.ngsildbroker.subscriptionmanager.service;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;

import io.vertx.mqtt.MqttClientOptions;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdConsts;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.collect.Table.Cell;
import com.google.common.net.HttpHeaders;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.EntityInfo;
import eu.neclab.ngsildbroker.commons.datatypes.NotificationParam;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteAttributeRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.DeleteSubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.InternalNotification;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.UpdateSubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.results.CRUDSuccess;
import eu.neclab.ngsildbroker.commons.datatypes.results.NGSILDOperationResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;
import eu.neclab.ngsildbroker.commons.tools.SubscriptionTools;
import eu.neclab.ngsildbroker.subscriptionmanager.messaging.SubscriptionSyncService;
import eu.neclab.ngsildbroker.subscriptionmanager.repository.SubscriptionInfoDAO;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.annotations.Broadcast;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.WebClient;
import io.vertx.mutiny.mqtt.MqttClient;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowIterator;
import io.vertx.pgclient.PgException;

@Singleton
public class SubscriptionService {

	private final static Logger logger = LoggerFactory.getLogger(SubscriptionService.class);

	@Inject
	SubscriptionInfoDAO subDAO;

	@Inject
	@Channel(AppConstants.INTERNAL_SUBS_CHANNEL)
	@Broadcast
	MutinyEmitter<SubscriptionRequest> internalSubEmitter;

	@Inject
	Vertx vertx;

	@RestClient
	@Inject
	LocalEntityService localEntityService;

	@RestClient
	@Inject
	LocalContextService localContextService;

	@Inject
	MicroServiceUtils microServiceUtils;
	private Table<String, String, SubscriptionRequest> tenant2subscriptionId2Subscription = HashBasedTable.create();
	private Table<String, String, SubscriptionRequest> tenant2subscriptionId2IntervalSubscription = HashBasedTable
			.create();
	private Map<String, SubscriptionRequest> subscriptionId2RequestGlobal = Maps.newHashMap();
	private HashMap<String, SubscriptionRequest> remoteNotifyCallbackId2InternalSub = new HashMap<String, SubscriptionRequest>();
	private HashMap<String, String> internalSubId2RemoteNotifyCallbackId2 = new HashMap<String, String>();
	private HashMap<String, String> internalSubId2ExternalEndpoint = new HashMap<String, String>();
	private WebClient webClient;

	private Map<String, MqttClient> host2MqttClient = Maps.newHashMap();

	private SubscriptionSyncService subscriptionSyncService = null;

	@PostConstruct
	void setup() {
		this.webClient = WebClient.create(vertx);
		subDAO.loadSubscriptions().onItem().transformToUni(subs -> {
			subs.forEach(tuple -> {
				SubscriptionRequest request;
				try {
					request = new SubscriptionRequest(tuple.getItem1(), tuple.getItem2(),
							new Context().parse(tuple.getItem3().get(NGSIConstants.JSON_LD_CONTEXT), false));
					if (isIntervalSub(request)) {
						this.tenant2subscriptionId2IntervalSubscription.put(request.getTenant(), request.getId(),
								request);
					} else {
						this.tenant2subscriptionId2Subscription.put(request.getTenant(), request.getId(), request);
					}
					subscriptionId2RequestGlobal.put(request.getId(), request);
				} catch (Exception e) {
					logger.error("Failed to load stored subscription " + tuple.getItem1());
				}
			});
			return Uni.createFrom().voidItem();
		}).await().indefinitely();
	}

	private boolean isIntervalSub(SubscriptionRequest request) {
		return request.getSubscription().getTimeInterval() > 0;
	}

	public Uni<NGSILDOperationResult> createSubscription(String tenant, Map<String, Object> subscription,
			Context contextLink) {
		SubscriptionRequest request;
		try {
			request = new SubscriptionRequest(tenant, subscription, contextLink);
		} catch (ResponseException e) {
			return Uni.createFrom().failure(e);
		}
		SubscriptionTools.setInitTimesSentAndFailed(request);
		Map<String, Object> tmp = request.getContext().serialize();

		return localContextService.createImplicitly(tenant, tmp).onItem().transformToUni(contextId -> {
			return subDAO.createSubscription(request, contextId).onItem().transformToUni(t -> {
				if (isIntervalSub(request)) {
					this.tenant2subscriptionId2IntervalSubscription.put(request.getTenant(), request.getId(), request);
				} else {
					tenant2subscriptionId2Subscription.put(tenant, request.getId(), request);
				}
				subscriptionId2RequestGlobal.put(request.getId(), request);
				Uni<Void> syncService;
				if (subscriptionSyncService != null) {
					syncService = subscriptionSyncService.sync(request);
				} else {
					syncService = Uni.createFrom().voidItem();
				}
				return syncService.onItem().transformToUni(v2 -> {
					return internalSubEmitter.send(request).onItem().transform(v -> {
						NGSILDOperationResult result = new NGSILDOperationResult(
								AppConstants.CREATE_SUBSCRIPTION_REQUEST, request.getId());
						result.addSuccess(new CRUDSuccess(null, null, request.getId(), Sets.newHashSet()));
						return result;
					});
				});
			}).onFailure().recoverWithUni(e -> {
				if (e instanceof PgException && ((PgException) e).getCode().equals(AppConstants.SQL_ALREADY_EXISTS)) {
					return Uni.createFrom().failure(new ResponseException(ErrorType.AlreadyExists,
							"Subscription with id " + request.getId() + " exists"));
				} else {
					return Uni.createFrom().failure(new ResponseException(ErrorType.InternalError, e.getMessage()));
				}

			});
		});
	}

	public Uni<NGSILDOperationResult> updateSubscription(String tenant, String subscriptionId,
			Map<String, Object> update, Context context) {
		UpdateSubscriptionRequest request = new UpdateSubscriptionRequest(tenant, subscriptionId, update, context);
		return localContextService.createImplicitly(tenant, request.getContext().serialize()).onItem()
				.transformToUni(contextId -> {
					return subDAO.updateSubscription(request, contextId).onItem().transformToUni(tup -> {
						if (tup.size() == 0) {
							return Uni.createFrom()
									.failure(new ResponseException(ErrorType.NotFound, "subscription not found"));
						}
						SubscriptionRequest updatedRequest;
						try {
							updatedRequest = new SubscriptionRequest(tenant, tup.getItem1(),
									new Context().parse(tup.getItem2(), false));
						} catch (Exception e) {
							return Uni.createFrom().failure(e);
						}

						if (isIntervalSub(updatedRequest)) {
							tenant2subscriptionId2IntervalSubscription.put(tenant, updatedRequest.getId(),
									updatedRequest);
							tenant2subscriptionId2Subscription.remove(tenant, updatedRequest.getId());
						} else {
							tenant2subscriptionId2Subscription.put(tenant, updatedRequest.getId(), updatedRequest);
							tenant2subscriptionId2IntervalSubscription.remove(tenant, updatedRequest.getId());
						}
						Uni<Void> syncService;
						if (subscriptionSyncService != null) {
							syncService = subscriptionSyncService.sync(updatedRequest);
						} else {
							syncService = Uni.createFrom().voidItem();
						}
						return syncService.onItem().transformToUni(v2 -> {
							return internalSubEmitter.send(updatedRequest).onItem().transform(v -> {
								return new NGSILDOperationResult(AppConstants.UPDATE_SUBSCRIPTION_REQUEST,
										request.getId());
							});
						});
					});
				});
	}

	public Uni<NGSILDOperationResult> deleteSubscription(String tenant, String subscriptionId) {
		DeleteSubscriptionRequest request = new DeleteSubscriptionRequest(tenant, subscriptionId);
		return subDAO.deleteSubscription(request).onItem().transformToUni(t -> {
			tenant2subscriptionId2IntervalSubscription.remove(tenant, subscriptionId);
			tenant2subscriptionId2Subscription.remove(tenant, subscriptionId);
			subscriptionId2RequestGlobal.remove(request.getId());
			Uni<Void> syncService;
			if (subscriptionSyncService != null) {
				syncService = subscriptionSyncService.sync(request);
			} else {
				syncService = Uni.createFrom().voidItem();
			}
			return syncService.onItem().transformToUni(v2 -> {
				return internalSubEmitter.send(request).onItem().transform(v -> {
					return new NGSILDOperationResult(AppConstants.DELETE_SUBSCRIPTION_REQUEST, request.getId());
				});
			});
		});
	}

	public Uni<QueryResult> getAllSubscriptions(String tenant, int limit, int offset) {
		return subDAO.getAllSubscriptions(tenant, limit, offset).onItem().transform(rows -> {
			QueryResult result = new QueryResult();
			Row next = null;
			RowIterator<Row> it = rows.iterator();
			List<Map<String, Object>> resultData = new ArrayList<>(rows.size());
			while (it.hasNext()) {
				next = it.next();
				resultData.add(next.getJsonObject(0).getMap());
			}
			result.setData(resultData);
			if (next == null) {
				return result;
			}
			long resultCount = rows.size();
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
		return subDAO.getSubscription(tenant, subscriptionId).onItem().transformToUni(rows -> {
			if (rows.size() == 0) {
				return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound, "subscription not found"));
			}
			return Uni.createFrom().item(rows.iterator().next().getJsonObject(0).getMap());
		});
	}

	public Uni<Void> checkSubscriptions(BaseRequest message) {
		Collection<SubscriptionRequest> potentialSubs = tenant2subscriptionId2Subscription.row(message.getTenant())
				.values();
		List<Uni<Void>> unis = Lists.newArrayList();
		logger.debug("checking subscriptions");
		for (SubscriptionRequest potentialSub : potentialSubs) {
			switch (message.getRequestType()) {
			case AppConstants.UPDATE_REQUEST, AppConstants.UPSERT_REQUEST, AppConstants.CREATE_REQUEST,
					AppConstants.APPEND_REQUEST ->
				unis.add(localEntityService.getAllByIds(message.getTenant(), message.getId(), true).onItem()
						.transformToUni(entityList -> {
							Map<String, Object> payload = new HashMap<>();
							payload.put(JsonLdConsts.GRAPH, entityList);
							return sendNotification(potentialSub, payload, message.getRequestType());
						}));
//TODO temp. commented because we need to check if the subscription is actually asking for it and the default is not to. so keeping default behaviour for now				
//			case AppConstants.DELETE_REQUEST -> {
//				unis.add(sendNotification(potentialSub, message.getPayload(), message.getRequestType()));
//			}
			case AppConstants.DELETE_ATTRIBUTE_REQUEST -> {
				if (shouldFire(Sets.newHashSet(((DeleteAttributeRequest) message).getAttribName()), potentialSub)) {
					unis.add(sendNotification(potentialSub, message.getPayload(), message.getRequestType()));
				}
			}
			default -> {
			}
			}
		}
		if (unis.isEmpty()) {
			return Uni.createFrom().voidItem();
		}
		return Uni.combine().all().unis(unis).discardItems();
	}

	private Uni<Void> sendNotification(SubscriptionRequest potentialSub, Map<String, Object> reg, int triggerReason) {
		List<Map<String, Object>> entityToBeSent = new ArrayList<>();
		if (triggerReason == AppConstants.DELETE_REQUEST) {
			List<String> ids = new ArrayList<>();
			Map<String, Object> idsMap = new HashMap<>();
			if (reg.containsKey(JsonLdConsts.GRAPH)) {
				for (Map<String, Object> entity : (List<Map<String, Object>>) reg.get(JsonLdConsts.GRAPH)) {
					if (shouldFire(entity.keySet(), potentialSub) && shouldSendOut(potentialSub, entity)) {
						ids.add((String) entity.get(JsonLdConsts.ID));
					}
				}
				idsMap.put(JsonLdConsts.GRAPH, ids);
				entityToBeSent.add(idsMap);
			} else if (shouldFire(reg.keySet(), potentialSub) && shouldSendOut(potentialSub, reg)) {
				ids.add((String) reg.get(JsonLdConsts.ID));
				idsMap.put(JsonLdConsts.GRAPH, ids);
				entityToBeSent.add(idsMap);
			}

		} else if (reg.containsKey(JsonLdConsts.GRAPH)) {
			for (Map<String, Object> entity : (List<Map<String, Object>>) reg.get(JsonLdConsts.GRAPH)) {
				if (shouldFire(entity.keySet(), potentialSub) && shouldSendOut(potentialSub, entity)) {
					entityToBeSent.add(entity);
				}
			}
		} else if (shouldSendOut(potentialSub, reg)) {
			entityToBeSent.add(reg);
		}
		if (!entityToBeSent.isEmpty()) {
			Map<String, Object> notification;
			try {
				notification = SubscriptionTools.generateNotification(potentialSub, entityToBeSent);
			} catch (Exception e) {
				logger.error("Failed to generate notification", e);
				return Uni.createFrom().voidItem();
			}
			NotificationParam notificationParam = potentialSub.getSubscription().getNotification();
			Uni<Void> toSend;
			switch (notificationParam.getEndPoint().getUri().getScheme()) {
			case "mqtt", "mqtts" -> {
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
									Buffer.buffer(SubscriptionTools.getMqttPayload(notificationParam, notification)),
									MqttQoS.valueOf(qos), false, false).onItem().transformToUni(t -> {
										if (t == 0) {
											// TODO what the fuck is the result here
										}
										long now = System.currentTimeMillis();
										potentialSub.getSubscription().getNotification()
												.setLastSuccessfulNotification(now);
										potentialSub.getSubscription().getNotification().setLastNotification(now);
										return subDAO.updateNotificationSuccess(potentialSub.getTenant(),
												potentialSub.getId(),
												SerializationTools.notifiedAt_formatter.format(LocalDateTime
														.ofInstant(Instant.ofEpochMilli(now), ZoneId.of("Z"))));
									}).onFailure().recoverWithUni(e -> {
										logger.error("failed to send notification for subscription " + potentialSub, e);
										long now = System.currentTimeMillis();
										potentialSub.getSubscription().getNotification().setLastFailedNotification(now);
										potentialSub.getSubscription().getNotification().setLastNotification(now);
										return subDAO.updateNotificationFailure(potentialSub.getTenant(),
												potentialSub.getId(),
												SerializationTools.notifiedAt_formatter.format(LocalDateTime
														.ofInstant(Instant.ofEpochMilli(now), ZoneId.of("Z"))));
									});
						} catch (Exception e) {
							logger.error("failed to send notification for subscription " + potentialSub, e);
							return Uni.createFrom().voidItem();
						}
					});
				} catch (Exception e) {
					logger.error("failed to send notification for subscription " + potentialSub, e);
					return Uni.createFrom().voidItem();
				}
			}
			case "http", "https" -> {
				try {
					toSend = webClient.postAbs(notificationParam.getEndPoint().getUri().toString())
							.putHeaders(SubscriptionTools.getHeaders(notificationParam))
							.sendBuffer(Buffer.buffer(JsonUtils.toPrettyString(notification))).onFailure().retry()
							.atMost(3).onItem().transformToUni(result -> {
								int statusCode = result.statusCode();
								long now = System.currentTimeMillis();
								if (statusCode >= 200 && statusCode < 300) {
									potentialSub.getSubscription().getNotification().setLastSuccessfulNotification(now);
									potentialSub.getSubscription().getNotification().setLastNotification(now);
									return subDAO.updateNotificationSuccess(potentialSub.getTenant(),
											potentialSub.getId(),
											SerializationTools.notifiedAt_formatter.format(LocalDateTime
													.ofInstant(Instant.ofEpochMilli(now), ZoneId.of("Z"))));
								} else {
									logger.error("failed to send notification for subscription " + potentialSub.getId()
											+ " with status code " + statusCode
											+ ". Remember there is no redirect following for post due to security considerations");
									potentialSub.getSubscription().getNotification().setLastFailedNotification(now);
									potentialSub.getSubscription().getNotification().setLastNotification(now);
									return subDAO.updateNotificationFailure(potentialSub.getTenant(),
											potentialSub.getId(),
											SerializationTools.notifiedAt_formatter.format(LocalDateTime
													.ofInstant(Instant.ofEpochMilli(now), ZoneId.of("Z"))));
								}
							}).onFailure().recoverWithUni(e -> {
								logger.error("failed to send notification for subscription " + potentialSub, e);
								long now = System.currentTimeMillis();
								potentialSub.getSubscription().getNotification().setLastFailedNotification(now);
								potentialSub.getSubscription().getNotification().setLastNotification(now);
								return subDAO.updateNotificationFailure(potentialSub.getTenant(), potentialSub.getId(),
										SerializationTools.notifiedAt_formatter.format(
												LocalDateTime.ofInstant(Instant.ofEpochMilli(now), ZoneId.of("Z"))));
							});
				} catch (Exception e) {
					logger.error("failed to send notification for subscription " + potentialSub, e);
					return Uni.createFrom().voidItem();
				}
			}
			default -> {
				logger.error("unsuported endpoint in subscription " + potentialSub.getId());
				return Uni.createFrom().voidItem();
			}
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

	private Uni<MqttClient> getMqttClient(NotificationParam notificationParam) {
		URI host = notificationParam.getEndPoint().getUri();
		String hostString = host.getUserInfo()+host.getHost() + host.getPort();
		MqttClient client;
		if (!host2MqttClient.containsKey(hostString)) {
			if(host.getUserInfo() != null){
				String[] usrPass = host.getUserInfo().split(":");
				client = MqttClient.create(vertx, new MqttClientOptions().setUsername(usrPass[0]).setPassword(usrPass[1]));
			}else{
				client = MqttClient.create(vertx, new MqttClientOptions());
			}
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

	@SuppressWarnings("unchecked")
	private boolean shouldSendOut(SubscriptionRequest potentialSub, Map<String, Object> entity) {
		Subscription sub = potentialSub.getSubscription();
		if (!sub.getIsActive() || sub.getExpiresAt() < System.currentTimeMillis()) {
			return false;
		}
		if (!SubscriptionTools.evaluateGeoQuery(sub.getLdGeoQuery(),
				(List<Map<String, Object>>) entity.get(NGSIConstants.NGSI_LD_LOCATION))) {
			return false;
		}
		if (sub.getScopeQuery() != null) {
			if (!sub.getScopeQuery().calculate(EntityTools.getScopes(entity))) {
				return false;
			}
		}
		if (sub.getLdQuery() != null) {
			if (!sub.getLdQuery().calculate(EntityTools.getBaseProperties(entity))) {
				return false;
			}
		}

		for (EntityInfo entityInfo : sub.getEntities()) {
			if (entityInfo.getId() != null && entityInfo.getType() != null && sub.getAttributeNames() != null) {
				if (checkEntityForIdTypeAttrs(entityInfo.getId(), entityInfo.getType(), sub.getAttributeNames(),
						entity)) {
					return true;
				}
			} else if (entityInfo.getIdPattern() != null && entityInfo.getType() != null
					&& sub.getAttributeNames() != null) {
				if (checkEntityForIdPatternTypeAttrs(entityInfo.getIdPattern(), entityInfo.getType(),
						sub.getAttributeNames(), entity)) {
					return true;
				}
			} else if (entityInfo.getId() != null && entityInfo.getType() != null) {
				if (checkEntityForIdType(entityInfo.getId(), entityInfo.getType(), entity)) {
					return true;
				}
			} else if (entityInfo.getIdPattern() != null && entityInfo.getType() != null) {
				if (checkEntityForIdPatternType(entityInfo.getIdPattern(), entityInfo.getType(), entity)) {
					return true;
				}
			} else if (entityInfo.getId() != null && sub.getAttributeNames() != null) {
				if (checkEntityForIdAttrs(entityInfo.getId(), sub.getAttributeNames(), entity)) {
					return true;
				}
			} else if (entityInfo.getIdPattern() != null && sub.getAttributeNames() != null) {
				if (checkEntityForIdPatternAttrs(entityInfo.getIdPattern(), sub.getAttributeNames(), entity)) {
					return true;
				}
			} else if (entityInfo.getType() != null && sub.getAttributeNames() != null) {
				if (checkEntityForTypeAttrs(entityInfo.getType(), sub.getAttributeNames(), entity)) {
					return true;
				}
			} else if (entityInfo.getType() != null) {
				if (checkEntityForType(entityInfo.getType(), entity)) {
					return true;
				}
			} else if (entityInfo.getIdPattern() != null) {
				if (checkEntityForIdPattern(entityInfo.getIdPattern(), entity)) {
					return true;
				}
			} else if (entityInfo.getId() != null) {
				if (checkEntityForId(entityInfo.getId(), entity)) {
					return true;
				}
			} else if (sub.getAttributeNames() != null) {
				if (checkEntityForAttribs(sub.getAttributeNames(), entity)) {
					return true;
				}
			}

		}

		return false;
	}

	private boolean checkEntityForId(URI id, Map<String, Object> entity) {
		return entity.get(NGSIConstants.JSON_LD_ID).equals(id.toString());
	}

	private boolean checkEntityForIdPattern(String idPattern, Map<String, Object> entity) {
		return ((String) entity.get(NGSIConstants.JSON_LD_ID)).matches(idPattern);
	}

	private boolean checkEntityForAttribs(Set<String> attributeNames, Map<String, Object> entity) {
		return !Sets.intersection(attributeNames, entity.keySet()).isEmpty();
	}

	@SuppressWarnings("unchecked")
	private boolean checkEntityForType(String type, Map<String, Object> entity) {
		return ((List<String>) entity.get(NGSIConstants.JSON_LD_TYPE)).contains(type);
	}

	private boolean checkEntityForTypeAttrs(String type, Set<String> attributeNames, Map<String, Object> entity) {
		return checkEntityForType(type, entity) && checkEntityForAttribs(attributeNames, entity);

	}

	private boolean checkEntityForIdPatternAttrs(String idPattern, Set<String> attributeNames,
			Map<String, Object> entity) {
		return checkEntityForIdPattern(idPattern, entity) && checkEntityForAttribs(attributeNames, entity);
	}

	private boolean checkEntityForIdAttrs(URI id, Set<String> attributeNames, Map<String, Object> entity) {
		return checkEntityForId(id, entity) && checkEntityForAttribs(attributeNames, entity);
	}

	private boolean checkEntityForIdPatternType(String idPattern, String type, Map<String, Object> entity) {
		return checkEntityForIdPattern(idPattern, entity) && checkEntityForType(type, entity);
	}

	private boolean checkEntityForIdType(URI id, String type, Map<String, Object> entity) {
		return checkEntityForId(id, entity) && checkEntityForType(type, entity);
	}

	private boolean checkEntityForIdPatternTypeAttrs(String idPattern, String type, Set<String> attributeNames,
			Map<String, Object> entity) {
		return checkEntityForIdPattern(idPattern, entity) && checkEntityForType(type, entity)
				&& checkEntityForAttribs(attributeNames, entity);
	}

	private boolean checkEntityForIdTypeAttrs(URI id, String type, Set<String> attributeNames,
			Map<String, Object> entity) {
		return checkEntityForId(id, entity) && checkEntityForType(type, entity)
				&& checkEntityForAttribs(attributeNames, entity);
	}

	private boolean shouldFire(Set<String> keys, SubscriptionRequest subscription) {
		if (subscription.getSubscription().getAttributeNames() == null
				|| subscription.getSubscription().getAttributeNames().isEmpty()) {
			return true;
		}

		for (String attribName : subscription.getSubscription().getAttributeNames()) {
			if (keys.contains(attribName)) {
				return true;
			}
		}
		return false;
	}

	@Scheduled(every = "${scorpio.subscription.checkinterval}", delayed = "${scorpio.startupdelay}")
	Uni<Void> checkIntervalSubs() {
		List<Uni<Void>> unis = Lists.newArrayList();
		for (Cell<String, String, SubscriptionRequest> cell : tenant2subscriptionId2IntervalSubscription.cellSet()) {
			SubscriptionRequest request = cell.getValue();
			Subscription sub = request.getSubscription();
			long now = System.currentTimeMillis();
			if (sub.getNotification().getLastNotification() + sub.getTimeInterval() < now) {

				unis.add(queryFromSubscription(request).onItem().transformToUni(queryResult -> {
					if (queryResult.isEmpty()) {
						return Uni.createFrom().voidItem();
					}
					try {
						return sendNotification(request, Map.of(JsonLdConsts.GRAPH, queryResult),
								AppConstants.INTERVAL_NOTIFICATION_REQUEST);
					} catch (Exception e) {
						logger.error("Failed to send initial notifcation", e);
						return Uni.createFrom().voidItem();
					}

				}));
			}
		}
		if (unis.isEmpty()) {
			return Uni.createFrom().voidItem();
		}
		return Uni.combine().all().unis(unis).combinedWith(list -> list).onItem()
				.transformToUni(list -> Uni.createFrom().voidItem());
	}

	private Uni<List<Map<String, Object>>> queryFromSubscription(SubscriptionRequest request) {
		Subscription sub = request.getSubscription();
		List<Uni<List<Map<String, Object>>>> unis = Lists.newArrayList();
		for (EntityInfo entityInfo : sub.getEntities()) {
			unis.add(localEntityService.query(request.getTenant(),
					entityInfo.getId() == null ? null : entityInfo.getId().toString(), entityInfo.getType(),
					entityInfo.getIdPattern(), StringUtils.join(sub.getAttributeNames(), ","), sub.getLdQueryString(),
					sub.getCsfQueryString(), sub.getLdGeoQuery() == null ? null : sub.getLdGeoQuery().getGeometry(),
					sub.getLdGeoQuery() == null ? null : sub.getLdGeoQuery().getGeorel(),
					sub.getLdGeoQuery() == null ? null : sub.getLdGeoQuery().getCoordinates(),
					sub.getLdGeoQuery() == null ? null : sub.getLdGeoQuery().getGeoproperty(), null, null,
					sub.getScopeQueryString(), false, null, null, true));
		}

		return Uni.combine().all().unis(unis).combinedWith(list -> {
			List<Map<String, Object>> result = Lists.newArrayList();
			for (Object obj : list) {
				List<Map<String, Object>> qResult = (List<Map<String, Object>>) obj;
				result.addAll(qResult);
			}
			return result;
		});
	}

	public Uni<Void> handleRegistryNotification(InternalNotification message) {
		if (message.getRequestType() == AppConstants.DELETE_REQUEST) {
			// entry was deleted
			return unsubscribeRemote(message.getId(), message.getTenant());
		} else {
			SubscriptionRequest subscriptionRequest = subscriptionId2RequestGlobal.get(message.getId());
			if (subscriptionRequest == null) {
				// this can happen when sub is already deleted but a notification for it still
				// arrives.
				return Uni.createFrom().voidItem();
			}
			SubscriptionRequest remoteRequest;
			try {
				remoteRequest = SubscriptionTools.generateRemoteSubscription(subscriptionRequest, message);
			} catch (ResponseException e) {
				logger.error("failed to generate a remote subscription", e);
				return Uni.createFrom().voidItem();
			}

			Map<String, Object> compacted;
			try {
				compacted = JsonLdProcessor.compact(remoteRequest.getPayload(), null, remoteRequest.getContext(),
						HttpUtils.opts, -1);
			} catch (Exception e) {
				logger.error("failed to generate remote subscription", e);
				return Uni.createFrom().voidItem();
			}
			prepareNotificationServlet(remoteRequest);
			if (message.getPayload().get(NGSIConstants.NGSI_LD_ENDPOINT) != null) {
				String remoteEndpoint = ((List<Map<String, String>>) message.getPayload()
						.get(NGSIConstants.NGSI_LD_ENDPOINT)).get(0).get(NGSIConstants.JSON_LD_VALUE);

				StringBuilder temp = new StringBuilder(remoteEndpoint);
				if (remoteEndpoint.endsWith("/")) {
					temp.deleteCharAt(remoteEndpoint.length() - 1);
				}
				temp.append(AppConstants.SUBSCRIPTIONS_URL);
				return webClient.post(temp.toString())
						.putHeaders(SubscriptionTools.getHeaders(remoteRequest.getSubscription().getNotification()))
						.sendJsonObject(new JsonObject(compacted)).onFailure().retry().atMost(3).onItem()
						.transformToUni(response -> {
							if (response.statusCode() >= 200 && response.statusCode() < 300) {
								String locationHeader = response.headers().get(HttpHeaders.LOCATION);
								// check if it's a relative path
								if (locationHeader.charAt(0) == '/') {
									locationHeader = remoteEndpoint + locationHeader;
								}
								internalSubId2ExternalEndpoint.put(subscriptionRequest.getSubscription().getId(),
										locationHeader);
							}
							return Uni.createFrom().voidItem();
						}).onFailure().recoverWithUni(t -> {
							logger.error("Failed to subscribe to remote host " + temp.toString(), t);
							return Uni.createFrom().voidItem();
						});
			} else
				return Uni.createFrom().voidItem();
		}
	}

	public Uni<Void> remoteNotify(String notificationEndpoint, Map<String, Object> notification, Context context) {
		SubscriptionRequest subscription = remoteNotifyCallbackId2InternalSub.get(notificationEndpoint);
		if (subscription == null) {
			return Uni.createFrom().voidItem();
		}
		List<Map<String, Object>> data = (List<Map<String, Object>>) notification.get(NGSIConstants.NGSI_LD_DATA);

		List<Uni<Void>> unis = Lists.newArrayList();
		for (Map<String, Object> entry : data) {
			if (shouldFire(entry.keySet(), subscription)) {
				unis.add(localEntityService
						.getEntityById(subscription.getTenant(), (String) entry.get(NGSIConstants.JSON_LD_ID), true)
						.onItem().transformToUni(entity -> {
							return sendNotification(subscription, entity, AppConstants.UPDATE_REQUEST);
						}));
			}
		}

		return Uni.combine().all().unis(unis).discardItems();
	}

	private String prepareNotificationServlet(SubscriptionRequest subToCheck) {

		String uuid = Long.toString(UUID.randomUUID().getLeastSignificantBits());
		remoteNotifyCallbackId2InternalSub.put(uuid, subToCheck);
		internalSubId2RemoteNotifyCallbackId2.put(subToCheck.getId(), uuid);
		StringBuilder url = new StringBuilder(microServiceUtils.getGatewayURL().toString()).append("/remotenotify/")
				.append(uuid);

		return url.toString();

	}

	private Uni<Void> unsubscribeRemote(String subscriptionId, String tenant) {
		String endpoint = internalSubId2ExternalEndpoint.remove(subscriptionId);
		if (endpoint != null) {
			remoteNotifyCallbackId2InternalSub.remove(internalSubId2RemoteNotifyCallbackId2.remove(subscriptionId));
			if (tenant != AppConstants.INTERNAL_NULL_KEY) {
				return webClient.deleteAbs(endpoint).putHeader(NGSIConstants.TENANT_HEADER, tenant).send().onItem()
						.transformToUni(t -> Uni.createFrom().voidItem());
			}
			return webClient.deleteAbs(endpoint).send().onItem().transformToUni(t -> Uni.createFrom().voidItem());
		}
		return Uni.createFrom().voidItem();
	}

	@PreDestroy
	public void unsubscribeToAllRemote() {
		List<Uni<Void>> unis = new ArrayList<>(internalSubId2ExternalEndpoint.values().size());
		for (String entry : internalSubId2ExternalEndpoint.values()) {
			logger.debug("Unsubscribing to remote host " + entry + " before shutdown");
			unis.add(webClient.deleteAbs(entry).send().onItem().transformToUni(t -> Uni.createFrom().voidItem()));
		}
		if (!unis.isEmpty()) {
			Uni.combine().all().unis(unis).discardItems().await().atMost(Duration.ofSeconds(30));
		}
	}

	public Uni<Void> syncCreateSubscription(SubscriptionRequest sub) {
		sub.getSubscription().setActive(false);
		if (isIntervalSub(sub)) {
			tenant2subscriptionId2IntervalSubscription.put(sub.getTenant(), sub.getId(), sub);
		} else {
			tenant2subscriptionId2Subscription.put(sub.getTenant(), sub.getId(), sub);
		}
		return Uni.createFrom().voidItem();
	}

	public Uni<Void> syncDeleteSubscription(SubscriptionRequest sub) {
		sub.getSubscription().setActive(false);
		if (isIntervalSub(sub)) {
			tenant2subscriptionId2IntervalSubscription.remove(sub.getTenant(), sub.getId());
		} else {
			tenant2subscriptionId2Subscription.remove(sub.getTenant(), sub.getId());
		}
		return Uni.createFrom().voidItem();
	}

	public Uni<Void> syncUpdateSubscription(SubscriptionRequest sub) {
		return subDAO.getSubscription(sub.getTenant(), sub.getId()).onFailure().recoverWithItem(e -> {
			if (isIntervalSub(sub)) {
				tenant2subscriptionId2IntervalSubscription.remove(sub.getTenant(), sub.getId());
			} else {
				tenant2subscriptionId2Subscription.remove(sub.getTenant(), sub.getId());
			}
			return null;
		}).onItem().transformToUni(rows -> {
			if (rows == null || rows.size() == 0) {
				return Uni.createFrom().voidItem();
			}
			String tenant = sub.getTenant();
			SubscriptionRequest updatedRequest;
			try {
				updatedRequest = new SubscriptionRequest(tenant, rows.iterator().next().getJsonObject(0).getMap(),
						sub.getContext());
			} catch (Exception e) {
				return Uni.createFrom().voidItem();
			}
			updatedRequest.getSubscription().setActive(false);
			if (isIntervalSub(updatedRequest)) {
				tenant2subscriptionId2IntervalSubscription.put(tenant, updatedRequest.getId(), updatedRequest);
				tenant2subscriptionId2Subscription.remove(tenant, updatedRequest.getId());
			} else {
				tenant2subscriptionId2Subscription.put(tenant, updatedRequest.getId(), updatedRequest);
				tenant2subscriptionId2IntervalSubscription.remove(tenant, updatedRequest.getId());
			}

			return Uni.createFrom().voidItem();
		});
	}

	public List<String> getAllSubscriptionIds() {
		Set<String> tmp = Sets.newHashSet(tenant2subscriptionId2Subscription.columnKeySet());
		tmp.addAll(tenant2subscriptionId2IntervalSubscription.columnKeySet());
		return tmp.stream().sorted().collect(Collectors.toList());
	}

	public void activateSubs(List<String> mySubs) {
		tenant2subscriptionId2Subscription.values().forEach(t -> {
			if (mySubs.contains(t.getId())) {
				t.getSubscription().setActive(true);
			} else {
				t.getSubscription().setActive(false);
			}
		});
		tenant2subscriptionId2IntervalSubscription.values().forEach(t -> {
			if (mySubs.contains(t.getId())) {
				t.getSubscription().setActive(true);
			} else {
				t.getSubscription().setActive(false);
			}
		});

	}

	public void addSyncService(SubscriptionSyncService subscriptionSyncService) {
		this.subscriptionSyncService = subscriptionSyncService;

	}

}
