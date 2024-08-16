package eu.neclab.ngsildbroker.subscriptionmanager.service;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.vertx.core.http.impl.headers.HeadersMultiMap;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLDService;
import com.github.jsonldjava.core.JsonLdConsts;
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
import eu.neclab.ngsildbroker.commons.datatypes.NotificationParam;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CSourceBaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.DeleteSubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.InternalNotification;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.UpdateSubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.results.CRUDSuccess;
import eu.neclab.ngsildbroker.commons.datatypes.results.NGSILDOperationResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;
import eu.neclab.ngsildbroker.commons.tools.SubscriptionTools;
import eu.neclab.ngsildbroker.subscriptionmanager.messaging.SyncService;
import eu.neclab.ngsildbroker.subscriptionmanager.repository.SubscriptionInfoDAO;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.quarkus.runtime.StartupEvent;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mqtt.MqttClientOptions;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.HttpRequest;
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
	JsonLDService ldService;

//	@Inject
//	@Channel(AppConstants.INTERNAL_SUBS_CHANNEL)
//	@Broadcast
//	MutinyEmitter<String> internalSubEmitter;

	@Inject
	ObjectMapper objectMapper;

	@ConfigProperty(name = "scorpio.messaging.maxSize")
	int messageSize;

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

	@ConfigProperty(name = "scorpio.alltypesub.type", defaultValue = "*")
	private String allTypeSubType;

	@ConfigProperty(name = "scorpio.entity-manager-server", defaultValue = "http://localhost:9090")
	private String entityServiceUrl;

	private String ALL_TYPES_SUB;

	private Table<String, String, SubscriptionRequest> tenant2subscriptionId2Subscription = HashBasedTable.create();
	private Table<String, String, SubscriptionRequest> tenant2subscriptionId2IntervalSubscription = HashBasedTable
			.create();
	private Map<String, SubscriptionRequest> subscriptionId2RequestGlobal = Maps.newHashMap();
	private HashMap<String, SubscriptionRequest> remoteNotifyCallbackId2InternalSub = new HashMap<String, SubscriptionRequest>();
	private HashMap<String, String> internalSubId2RemoteNotifyCallbackId2 = new HashMap<String, String>();
	private HashMap<String, String> internalSubId2ExternalEndpoint = new HashMap<String, String>();
	private WebClient webClient;

	private Map<String, MqttClient> host2MqttClient = Maps.newHashMap();
	private SyncService subscriptionSyncService = null;

	private static Map<String, Object> compareMaps(Map<String, Object> oldMap, Map<String, Object> newMap) {
		if (oldMap == null || oldMap.isEmpty()) {
			return newMap;
		}
		Map<String, Object> resultMap = new HashMap<>();
		for (Map.Entry<String, Object> entry : newMap.entrySet()) {
			String key = entry.getKey();
			if (key.equals(NGSIConstants.JSON_LD_ID) || key.equals(JsonLdConsts.TYPE)) {
				resultMap.put(key, newMap.get(key));
				continue;
			}

			Object newValue = entry.getValue();
			Object oldValue = oldMap.get(key);

			if (!isEqual(oldValue, newValue)) {
				addProperty(resultMap, key, oldValue, newValue);
			} else {
				resultMap.put(key, newValue);
			}
		}
		for (Map.Entry<String, Object> entry : oldMap.entrySet()) {
			String key = entry.getKey();
			if (key.equals(NGSIConstants.JSON_LD_ID) || key.equals(NGSIConstants.JSON_LD_TYPE)) {
				continue;
			}
			if (!resultMap.containsKey(key)) {
				addProperty(resultMap, key, oldMap.get(key),
						List.of(Map.of(NGSIConstants.NGSI_LD_HAS_VALUE,
								List.of(Map.of(JsonLdConsts.VALUE, NGSIConstants.NGSI_LD_NULL)), JsonLdConsts.TYPE,
								((List<Map<String, Object>>) oldMap.get(key)).get(0).get(JsonLdConsts.TYPE))));
			}
		}
		return resultMap;
	}

	private static boolean isEqual(Object obj1, Object obj2) {
		if (obj1 == null || obj2 == null) {
			return obj1 == obj2;
		}
		return obj1.equals(obj2);
	}

	private static void addProperty(Map<String, Object> resultMap, String key, Object oldValue, Object newValue) {
		Map<String, Object> propertyMap = new HashMap<>();
		List<Object> valueList = List.of(propertyMap);

		propertyMap.put(JsonLdConsts.TYPE, ((List<Map<String, Object>>) newValue).get(0).get(JsonLdConsts.TYPE));
		if (key.equals(NGSIConstants.NGSI_LD_MODIFIED_AT)) {
			propertyMap.put(JsonLdConsts.VALUE, ((List<Map<String, Object>>) newValue).get(0).get(JsonLdConsts.VALUE));
		}
		if (((List<Map<String, Object>>) newValue).get(0).containsKey(NGSIConstants.NGSI_LD_HAS_VALUE)) {
			propertyMap.put(NGSIConstants.VALUE,
					((List<Map<String, Object>>) newValue).get(0).get(NGSIConstants.NGSI_LD_HAS_VALUE));
		}
		if (((List<Map<String, Object>>) newValue).get(0).containsKey(NGSIConstants.NGSI_LD_HAS_LANGUAGE_MAP)) {
			propertyMap.put(NGSIConstants.LANGUAGE_MAP,
					((List<Map<String, Object>>) newValue).get(0).get(NGSIConstants.NGSI_LD_HAS_LANGUAGE_MAP));
		}
		if (((List<Map<String, Object>>) newValue).get(0).containsKey(NGSIConstants.NGSI_LD_HAS_OBJECT)) {
			propertyMap.put(NGSIConstants.OBJECT,
					((List<Map<String, Object>>) newValue).get(0).get(NGSIConstants.NGSI_LD_HAS_OBJECT));
		}
		if (((List<Map<String, Object>>) newValue).get(0).containsKey(NGSIConstants.NGSI_LD_HAS_OBJECT_LIST)) {
			propertyMap.put(NGSIConstants.OBJECT_LIST,
					((List<Map<String, Object>>) newValue).get(0).get(NGSIConstants.NGSI_LD_HAS_OBJECT_LIST));
		}
		if (((List<Map<String, Object>>) newValue).get(0).containsKey(NGSIConstants.NGSI_LD_HAS_VOCAB)) {
			propertyMap.put(NGSIConstants.VOCAB,
					((List<Map<String, Object>>) newValue).get(0).get(NGSIConstants.NGSI_LD_HAS_VOCAB));
		}
		if (((List<Map<String, Object>>) newValue).get(0).containsKey(NGSIConstants.NGSI_LD_HAS_LIST)) {
			propertyMap.put(NGSIConstants.VALUE_LIST,
					((List<Map<String, Object>>) newValue).get(0).get(NGSIConstants.NGSI_LD_HAS_LIST));
		}
		if (((List<Map<String, Object>>) newValue).get(0).containsKey(NGSIConstants.NGSI_LD_HAS_JSON)) {
			propertyMap.put(NGSIConstants.JSON,
					((List<Map<String, Object>>) newValue).get(0).get(NGSIConstants.NGSI_LD_HAS_JSON));
		}
		if (((List<Map<String, Object>>) newValue).get(0).containsKey(NGSIConstants.NGSI_LD_UNIT_CODE)) {
			propertyMap.put(NGSIConstants.QUERY_PARAMETER_UNIT_CODE,
					((List<Map<String, Object>>) newValue).get(0).get(NGSIConstants.NGSI_LD_UNIT_CODE));
		}
		if (((List<Map<String, Object>>) newValue).get(0).containsKey(NGSIConstants.NGSI_LD_CREATED_AT)) {
			propertyMap.put(NGSIConstants.CREATEDAT,
					((List<Map<String, Object>>) newValue).get(0).get(NGSIConstants.NGSI_LD_CREATED_AT));
		}
		if (((List<Map<String, Object>>) newValue).get(0).containsKey(NGSIConstants.NGSI_LD_MODIFIED_AT)) {
			propertyMap.put(NGSIConstants.QUERY_PARAMETER_MODIFIED_AT,
					((List<Map<String, Object>>) newValue).get(0).get(NGSIConstants.NGSI_LD_MODIFIED_AT));
		}
		if (((List<Map<String, Object>>) newValue).get(0).containsKey(NGSIConstants.NGSI_LD_PROVIDED_BY)) {
			propertyMap.put(NGSIConstants.PROVIDED_BY,
					((List<Map<String, Object>>) newValue).get(0).get(NGSIConstants.NGSI_LD_PROVIDED_BY));
		}

		if (oldValue != null) {
			if (((List<Map<String, Object>>) oldValue).get(0).containsKey(NGSIConstants.NGSI_LD_HAS_VALUE)) {
				propertyMap.put(NGSIConstants.PREVIOUS_VALUE,
						((List<Map<String, Object>>) oldValue).get(0).get(NGSIConstants.NGSI_LD_HAS_VALUE));
			}
			if (((List<Map<String, Object>>) oldValue).get(0).containsKey(NGSIConstants.NGSI_LD_HAS_LANGUAGE_MAP)) {
				propertyMap.put(NGSIConstants.PREVIOUS_LANGUAGE_MAP,
						((List<Map<String, Object>>) oldValue).get(0).get(NGSIConstants.NGSI_LD_HAS_LANGUAGE_MAP));
			}
			if (((List<Map<String, Object>>) oldValue).get(0).containsKey(NGSIConstants.NGSI_LD_HAS_OBJECT)) {
				propertyMap.put(NGSIConstants.PREVIOUS_OBJECT,
						((List<Map<String, Object>>) oldValue).get(0).get(NGSIConstants.NGSI_LD_HAS_OBJECT));
			}
			if (((List<Map<String, Object>>) oldValue).get(0).containsKey(NGSIConstants.NGSI_LD_HAS_OBJECT_LIST)) {
				propertyMap.put(NGSIConstants.PREVIOUS_OJBECT_LIST,
						((List<Map<String, Object>>) oldValue).get(0).get(NGSIConstants.NGSI_LD_HAS_OBJECT_LIST));
			}
			if (((List<Map<String, Object>>) oldValue).get(0).containsKey(NGSIConstants.NGSI_LD_HAS_VOCAB)) {
				propertyMap.put(NGSIConstants.PREVIOUS_VOCAB,
						((List<Map<String, Object>>) oldValue).get(0).get(NGSIConstants.NGSI_LD_HAS_VOCAB));
			}
			if (((List<Map<String, Object>>) oldValue).get(0).containsKey(NGSIConstants.NGSI_LD_HAS_LIST)) {
				propertyMap.put(NGSIConstants.PREVIOUS_VALUE_LIST,
						((List<Map<String, Object>>) oldValue).get(0).get(NGSIConstants.NGSI_LD_HAS_LIST));
			}
			if (((List<Map<String, Object>>) oldValue).get(0).containsKey(NGSIConstants.NGSI_LD_HAS_JSON)) {
				propertyMap.put(NGSIConstants.PREVIOUS_JSON,
						((List<Map<String, Object>>) oldValue).get(0).get(NGSIConstants.NGSI_LD_HAS_JSON));
			}
		}

		resultMap.put(key, valueList);
	}

	void startup(@Observes StartupEvent event) {
	}

	@PostConstruct
	void setup() {
		this.webClient = WebClient.create(vertx);
		ALL_TYPES_SUB = NGSIConstants.NGSI_LD_DEFAULT_PREFIX + allTypeSubType;
		subDAO.loadSubscriptions().onItem().transformToUni(subs -> {
			List<Uni<Tuple2<Tuple2<String, Map<String, Object>>, Context>>> unis = Lists.newArrayList();
			subs.forEach(tuple -> {
				unis.add(ldService.parsePure(tuple.getItem3().get(NGSIConstants.JSON_LD_CONTEXT)).onItem()
						.transform(ctx -> {
							return Tuple2.of(Tuple2.of(tuple.getItem1(), tuple.getItem2()), ctx);
						}));
			});
			if (unis.isEmpty()) {
				return Uni.createFrom().voidItem();
			}
			return Uni.combine().all().unis(unis).combinedWith(list -> {
				for (Object obj : list) {
					Tuple2<Tuple2<String, Map<String, Object>>, Context> tuple = (Tuple2<Tuple2<String, Map<String, Object>>, Context>) obj;
					SubscriptionRequest request;
					try {
						request = new SubscriptionRequest(tuple.getItem1().getItem1(), tuple.getItem1().getItem2(),
								tuple.getItem2());
						request.getSubscription().addOtherHead(NGSIConstants.LINK_HEADER,
								"<%s>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\""
										.formatted(request.getSubscription().getJsonldContext()));
						request.getSubscription().addOtherHead(NGSIConstants.TENANT_HEADER, request.getTenant());
						request.setSendTimestamp(-1);
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
				}
				return null;

			});
		}).await().indefinitely();
	}

	private boolean isIntervalSub(SubscriptionRequest request) {
		return request.getSubscription().getTimeInterval() > 0;
	}

	public Uni<NGSILDOperationResult> createSubscription(HeadersMultiMap linkHead, String tenant,
			Map<String, Object> subscription, Context contextLink) {
		SubscriptionRequest request;
		if (!subscription.containsKey(NGSIConstants.JSON_LD_ID)) {
			String id = "urn:" + UUID.randomUUID();
			subscription.put(NGSIConstants.JSON_LD_ID, id);
		}
		try {
			request = new SubscriptionRequest(tenant, subscription, contextLink);
			request.getSubscription().setOtherHead(linkHead);
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
					logger.info("sync service");
					syncService = subscriptionSyncService.sync(request);
				} else {
					logger.info("No sync service");
					syncService = Uni.createFrom().voidItem();
				}
				return syncService.onItem().transform(v2 -> {
//					try {
//						MicroServiceUtils.serializeAndSplitObjectAndEmit(request, messageSize, internalSubEmitter,
//								objectMapper);
//					} catch (ResponseException e) {
//						logger.error("Failed to serialize subscription message", e);
//					}
					NGSILDOperationResult result = new NGSILDOperationResult(AppConstants.CREATE_SUBSCRIPTION_REQUEST,
							request.getId());
					result.addSuccess(new CRUDSuccess(null, null, request.getId(), Sets.newHashSet()));
					return result;

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
						return ldService.parsePure(tup.getItem2()).onItem().transformToUni(ctx -> {
							SubscriptionRequest updatedRequest;
							try {
								updatedRequest = new SubscriptionRequest(tenant, subscriptionId, tup.getItem1(), ctx,
										false);
							} catch (Exception e) {
								return Uni.createFrom().failure(e);
							}
							Uni<Void> syncService;
							if (subscriptionSyncService != null) {
								syncService = subscriptionSyncService.sync(updatedRequest);
							} else {
								syncService = Uni.createFrom().voidItem();
							}
							return syncService.onItem().transform(v2 -> {
								if (isIntervalSub(updatedRequest)) {
									tenant2subscriptionId2IntervalSubscription.put(tenant, updatedRequest.getId(),
											updatedRequest);
									subscriptionId2RequestGlobal.put(updatedRequest.getId(), updatedRequest);
									tenant2subscriptionId2Subscription.remove(tenant, updatedRequest.getId());
								} else {
									tenant2subscriptionId2Subscription.put(tenant, updatedRequest.getId(),
											updatedRequest);
									subscriptionId2RequestGlobal.put(updatedRequest.getId(), updatedRequest);
									tenant2subscriptionId2IntervalSubscription.remove(tenant, updatedRequest.getId());
								}
//								try {
//									MicroServiceUtils.serializeAndSplitObjectAndEmit(updatedRequest, messageSize,
//											internalSubEmitter, objectMapper);
//								} catch (ResponseException e) {
//									logger.error("Failed to serialize subscription message", e);
//								}
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
			return syncService.onItem().transform(v2 -> {
//				try {
//					MicroServiceUtils.serializeAndSplitObjectAndEmit(request, messageSize, internalSubEmitter,
//							objectMapper);
//				} catch (ResponseException e) {
//					logger.error("Failed to serialize subscription message", e);
//				}
				return new NGSILDOperationResult(AppConstants.DELETE_SUBSCRIPTION_REQUEST, request.getId());
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
				Map<String, Object> subscriptionData = next.getJsonObject(0).getMap();
				String subscriptionId = (String) subscriptionData.get(NGSIConstants.JSON_LD_ID);
				SubscriptionRequest subscriptionRequest = subscriptionId2RequestGlobal.get(subscriptionId);
				if (subscriptionRequest != null) {
					subscriptionData.put(NGSIConstants.STATUS, subscriptionRequest.getSubscription().getStatus());
				}
				resultData.add(subscriptionData);
				// resultData.add(next.getJsonObject(0).getMap());
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
			Map<String, Object> rowData = rows.iterator().next().getJsonObject(0).getMap();
			rowData.put(NGSIConstants.STATUS,
					subscriptionId2RequestGlobal.get(subscriptionId).getSubscription().getStatus());
			return Uni.createFrom().item(rowData);
		});
	}

	public Uni<Void> checkSubscriptions(BaseRequest message) {
		Collection<SubscriptionRequest> potentialSubs = tenant2subscriptionId2Subscription.row(message.getTenant())
				.values();
		List<Uni<Void>> unis = Lists.newArrayList();
		logger.debug("checking subscriptions");

		for (SubscriptionRequest potentialSub : potentialSubs) {
			List<Map<String, Object>> dataToSend = Lists.newArrayList();

			if ((potentialSub.getSendTimestamp() != -1 && potentialSub.getSendTimestamp() > message.getSendTimestamp())
					|| !notificationTriggerCheck(potentialSub.getSubscription(), message.getRequestType())) {
				continue;
			}
			Map<String, List<Map<String, Object>>> payloadToUse = Maps.newHashMap();
			Map<String, List<Map<String, Object>>> prevPayloadToUse = Maps.newHashMap();
			if (message.getPayload() != null) {
				for (Entry<String, List<Map<String, Object>>> entry : message.getPayload().entrySet()) {
					for (Map<String, Object> mapEntry : entry.getValue()) {
						if (potentialSub.firstCheckToSendOut(entry.getKey(), mapEntry, ALL_TYPES_SUB)) {
							payloadToUse.put(entry.getKey(), entry.getValue());
							if (message.getPrevPayload() != null) {
								prevPayloadToUse.put(entry.getKey(), message.getPrevPayload().get(entry.getKey()));
							}

						}
					}
				}
				if (payloadToUse.isEmpty()) {
					continue;
				}
			} else if (message.getPrevPayload() != null) {
				for (Entry<String, List<Map<String, Object>>> entry : message.getPrevPayload().entrySet()) {
					for (Map<String, Object> mapEntry : entry.getValue()) {
						if (potentialSub.firstCheckToSendOut(entry.getKey(), mapEntry, ALL_TYPES_SUB)) {
							prevPayloadToUse.put(entry.getKey(), entry.getValue());
						}
					}
				}
				if (prevPayloadToUse.isEmpty()) {
					continue;
				}
			} else {
				continue;
			}
			
			if (message.isDistributed() || potentialSub.doJoin()) {
				Set<String> idsTbu;
				if (!payloadToUse.isEmpty()) {
					idsTbu = payloadToUse.keySet();
				} else if (!prevPayloadToUse.isEmpty()) {
					idsTbu = prevPayloadToUse.keySet();
				} else {
					idsTbu = message.getIds();
				}
				unis.add(queryFromSubscription(potentialSub, message.getTenant(), idsTbu, prevPayloadToUse).onItem()
						.transformToUni(tbs -> {
							List<Map<String, Object>> toAddLater = Lists.newArrayList();
							Iterator<Map<String, Object>> it = tbs.iterator();
							while (it.hasNext()) {
								Map<String, Object> entity = it.next();
								String entityId = (String) entity.get(NGSIConstants.JSON_LD_ID);
								List<Map<String, Object>> payload = payloadToUse.get(entityId);
								if (payload != null) {
									if (payload.size() == 1) {
										entity.putAll(payload.get(0));
										if (potentialSub.getSubscription().getNotification().getShowChanges()) {
											it.remove();
											toAddLater.add(compareMaps(prevPayloadToUse.get(entityId).get(0), entity));
										}
									} else {
										it.remove();
										for (int i = 0; i < payload.size(); i++) {
											Map<String, Object> pEntry = payload.get(0);
											Map<String, Object> dupl = MicroServiceUtils.deepCopyMap(entity);
											dupl.putAll(pEntry);
											if (potentialSub.getSubscription().getNotification().getShowChanges()) {
												List<Map<String, Object>> prev = prevPayloadToUse.get(entityId);
												if (prev != null && i < prev.size()) {
													dupl = compareMaps(prev.get(i), dupl);
												}

											}
											toAddLater.add(dupl);
										}
									}
								}
							}
							tbs.addAll(toAddLater);
							return sendNotification(potentialSub, tbs);
						}));

			} else {
				switch (message.getRequestType()) {
				case AppConstants.BATCH_CREATE_REQUEST:
				case AppConstants.CREATE_REQUEST: {
					for (Entry<String, List<Map<String, Object>>> entry : payloadToUse.entrySet()) {
						List<Map<String, Object>> entryList = entry.getValue();
						for (int i = 0; i < entryList.size(); i++) {
							Map<String, Object> mapEntry = entryList.get(i);

							Map<String, Object> dupl = MicroServiceUtils.deepCopyMap(mapEntry);
							if (potentialSub.fullEntityCheckToSendOut(entry.getKey(), dupl, allTypeSubType, null)) {
								dataToSend.add(dupl);
							}
						}
					}

					break;
				}
				case AppConstants.BATCH_UPDATE_REQUEST:
				case AppConstants.BATCH_UPSERT_REQUEST:
				case AppConstants.APPEND_REQUEST:
				case AppConstants.UPDATE_REQUEST:
				case AppConstants.BATCH_MERGE_REQUEST:
				case AppConstants.REPLACE_ATTRIBUTE_REQUEST:
				case AppConstants.REPLACE_ENTITY_REQUEST:
				case AppConstants.MERGE_PATCH_REQUEST: {
					dataToSend = mergePrevAndNew(payloadToUse, prevPayloadToUse,
							potentialSub.getSubscription().getNotification().getShowChanges());
					break;
				}
				default:
					break;
				}

			}
			unis.add(sendNotification(potentialSub, dataToSend));
		}
		if (unis.isEmpty()) {
			return Uni.createFrom().voidItem();
		}
		return Uni.combine().all().unis(unis).discardItems();
	}

	private List<Map<String, Object>> mergePrevAndNew(Map<String, List<Map<String, Object>>> payloadToUse,
			Map<String, List<Map<String, Object>>> prevPayloadToUse, Boolean showChanges) {
		List<Map<String, Object>> result = Lists.newArrayList();
		for (Entry<String, List<Map<String, Object>>> entry : payloadToUse.entrySet()) {
			String entityId = entry.getKey();
			List<Map<String, Object>> newValues = entry.getValue();
			if (prevPayloadToUse == null) {
				result.addAll(newValues);
			} else {
				List<Map<String, Object>> oldValues = prevPayloadToUse.get(entityId);
				if (oldValues == null) {
					result.addAll(newValues);
				} else {
					for (int i = 0; i < newValues.size(); i++) {
						Map<String, Object> newValue = newValues.get(i);
						if (i >= oldValues.size()) {
							result.addAll(newValues);
						} else {
							Map<String, Object> oldValue = oldValues.get(i);
							if (oldValue == null) {
								result.addAll(newValues);
							} else {
								result.add(mergePrevAndNewEntity(oldValue, newValue, showChanges));
							}
						}

					}
				}
			}
		}
		return result;
	}

	private Map<String, Object> mergePrevAndNewEntity(Map<String, Object> oldValue, Map<String, Object> newValue,
			Boolean showChanges) {
		Map<String, Object> result = MicroServiceUtils.deepCopyMap(oldValue);
		Object modifiedAt;
		for (Entry<String, Object> entry : newValue.entrySet()) {
			String attribName = entry.getKey();
			switch (attribName) {
			case NGSIConstants.JSON_LD_ID:
				continue;
			case NGSIConstants.JSON_LD_TYPE:
				List<String> mergedTypes = Stream
						.concat(((List<String>) result.get(NGSIConstants.JSON_LD_TYPE)).stream(),
								((List<String>) entry.getValue()).stream())
						.distinct().collect(Collectors.toList());
				result.put(attribName, mergedTypes);
				break;
			case NGSIConstants.NGSI_LD_SCOPE:
				result.put(attribName, entry.getValue());
				break;
			default:
				if (showChanges) {
					List<Map<String, Object>> oldValues = (List<Map<String, Object>>) result.get(attribName);
					List<Map<String, Object>> newValues = (List<Map<String, Object>>) entry.getValue();
					if (oldValues == null) {
						result.put(attribName, entry.getValue());
					} else {
						for (Map<String, Object> newEntry : newValues) {
							Object newDatasetId = newEntry.get(NGSIConstants.NGSI_LD_DATA_SET_ID);
							Iterator<Map<String, Object>> it = oldValues.iterator();
							boolean found = false;
							while (it.hasNext()) {
								Map<String, Object> oldEntry = it.next();
								Object oldDatasetId = oldEntry.get(NGSIConstants.NGSI_LD_DATA_SET_ID);
								if (oldDatasetId == null && newDatasetId == null) {
									it.remove();
									mergeAttrib(newEntry, oldEntry);
									found = true;
									break;
								}
								if (oldDatasetId != null && newDatasetId != null && oldDatasetId.equals(newDatasetId)) {
									it.remove();
									mergeAttrib(newEntry, oldEntry);
									found = true;
									break;
								}
							}
							if (!found) {
								mergeAttribToNone(newEntry);
							}
							oldValues.add(newEntry);
						}
					}
				} else {
					result.put(attribName, entry.getValue());
				}
				break;
			}
		}
		return result;
	}

	private void mergeAttribToNone(Map<String, Object> newEntry) {
		if (newEntry.containsKey(NGSIConstants.NGSI_LD_HAS_VALUE)) {
			newEntry.put(NGSIConstants.PREVIOUS_VALUE,
					List.of(Map.of(NGSIConstants.JSON_LD_VALUE, NGSIConstants.JSON_LD_NONE)));
		} else if (newEntry.containsKey(NGSIConstants.NGSI_LD_HAS_OBJECT)) {
			newEntry.put(NGSIConstants.PREVIOUS_OBJECT,
					List.of(Map.of(NGSIConstants.JSON_LD_ID, NGSIConstants.JSON_LD_NONE)));
		} else if (newEntry.containsKey(NGSIConstants.NGSI_LD_HAS_JSON)) {
			newEntry.put(NGSIConstants.PREVIOUS_JSON, List.of(Map.of(NGSIConstants.JSON_LD_TYPE,
					NGSIConstants.JSON_LD_JSON, NGSIConstants.JSON_LD_VALUE, NGSIConstants.JSON_LD_NONE)));
		} else if (newEntry.containsKey(NGSIConstants.NGSI_LD_HAS_LANGUAGE_MAP)) {
			newEntry.put(NGSIConstants.PREVIOUS_LANGUAGE_MAP,
					List.of(Map.of(NGSIConstants.JSON_LD_VALUE, NGSIConstants.JSON_LD_NONE)));
		} else if (newEntry.containsKey(NGSIConstants.NGSI_LD_HAS_OBJECT_LIST)) {
			newEntry.put(NGSIConstants.PREVIOUS_OJBECT_LIST,
					List.of(Map.of(NGSIConstants.JSON_LD_LIST, NGSIConstants.JSON_LD_NONE)));
		} else if (newEntry.containsKey(NGSIConstants.NGSI_LD_HAS_LIST)) {
			newEntry.put(NGSIConstants.PREVIOUS_VALUE_LIST,
					List.of(Map.of(NGSIConstants.JSON_LD_LIST, NGSIConstants.JSON_LD_NONE)));
		} else if (newEntry.containsKey(NGSIConstants.NGSI_LD_HAS_VOCAB)) {
			newEntry.put(NGSIConstants.PREVIOUS_VOCAB,
					List.of(Map.of(NGSIConstants.JSON_LD_ID, NGSIConstants.JSON_LD_NONE)));
		}

	}

	private void mergeAttrib(Map<String, Object> newEntry, Map<String, Object> oldEntry) {
		if (oldEntry.containsKey(NGSIConstants.NGSI_LD_HAS_VALUE)) {
			newEntry.put(NGSIConstants.PREVIOUS_VALUE, oldEntry.get(NGSIConstants.NGSI_LD_HAS_VALUE));
		} else if (oldEntry.containsKey(NGSIConstants.NGSI_LD_HAS_OBJECT)) {
			newEntry.put(NGSIConstants.PREVIOUS_OBJECT, oldEntry.get(NGSIConstants.NGSI_LD_HAS_OBJECT));
		} else if (oldEntry.containsKey(NGSIConstants.NGSI_LD_HAS_JSON)) {
			newEntry.put(NGSIConstants.PREVIOUS_JSON, oldEntry.get(NGSIConstants.NGSI_LD_HAS_JSON));
		} else if (oldEntry.containsKey(NGSIConstants.NGSI_LD_HAS_LANGUAGE_MAP)) {
			newEntry.put(NGSIConstants.PREVIOUS_LANGUAGE_MAP, oldEntry.get(NGSIConstants.NGSI_LD_HAS_LANGUAGE_MAP));
		} else if (oldEntry.containsKey(NGSIConstants.NGSI_LD_HAS_OBJECT_LIST)) {
			newEntry.put(NGSIConstants.PREVIOUS_OJBECT_LIST, oldEntry.get(NGSIConstants.NGSI_LD_HAS_OBJECT_LIST));
		} else if (oldEntry.containsKey(NGSIConstants.NGSI_LD_HAS_LIST)) {
			newEntry.put(NGSIConstants.PREVIOUS_VALUE_LIST, oldEntry.get(NGSIConstants.NGSI_LD_HAS_LIST));
		} else if (oldEntry.containsKey(NGSIConstants.NGSI_LD_HAS_VOCAB)) {
			newEntry.put(NGSIConstants.PREVIOUS_VOCAB, oldEntry.get(NGSIConstants.NGSI_LD_HAS_VOCAB));
		}

	}

	private Uni<Void> sendNotification(SubscriptionRequest potentialSub, List<Map<String, Object>> dataToSend) {
		if (dataToSend.isEmpty()) {
			return Uni.createFrom().voidItem();
		}
		return SubscriptionTools.generateNotification(potentialSub, dataToSend, ldService).onItem()
				.transformToUni(notification -> {
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
									return client
											.publish(notificationParam.getEndPoint().getUri().getPath().substring(1),
													Buffer.buffer(SubscriptionTools.getMqttPayload(notificationParam,
															notification)),
													MqttQoS.valueOf(qos), false, false)
											.onItem().transformToUni(t -> {
												if (t == 0) {
													// TODO what the fuck is the result here
												}
												long now = System.currentTimeMillis();
												potentialSub.getSubscription().getNotification()
														.setLastSuccessfulNotification(now);
												potentialSub.getSubscription().getNotification()
														.setLastNotification(now);
												return subDAO.updateNotificationSuccess(potentialSub.getTenant(),
														potentialSub.getId(),
														SerializationTools.notifiedAt_formatter.format(LocalDateTime
																.ofInstant(Instant.ofEpochMilli(now), ZoneId.of("Z"))));
											}).onFailure().recoverWithUni(e -> {
												logger.error(
														"failed to send notification for subscription " + potentialSub,
														e);
												long now = System.currentTimeMillis();
												potentialSub.getSubscription().getNotification()
														.setLastFailedNotification(now);
												potentialSub.getSubscription().getNotification()
														.setLastNotification(now);
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
									.putHeaders(SubscriptionTools.getHeaders(notificationParam,
											potentialSub.getSubscription().getOtherHead()))
									.sendBuffer(Buffer.buffer(JsonUtils.toPrettyString(notification))).onFailure()
									.retry().atMost(3).onItem().transformToUni(result -> {
										int statusCode = result.statusCode();
										long now = System.currentTimeMillis();
										if (statusCode >= 200 && statusCode < 300) {
											potentialSub.getSubscription().getNotification()
													.setLastSuccessfulNotification(now);
											potentialSub.getSubscription().getNotification().setLastNotification(now);
											return subDAO.updateNotificationSuccess(potentialSub.getTenant(),
													potentialSub.getId(),
													SerializationTools.notifiedAt_formatter.format(LocalDateTime
															.ofInstant(Instant.ofEpochMilli(now), ZoneId.of("Z"))));
										} else {
											logger.error("failed to send notification for subscription "
													+ potentialSub.getId() + " with status code " + statusCode
													+ ". Remember there is no redirect following for post due to security considerations");
											potentialSub.getSubscription().getNotification()
													.setLastFailedNotification(now);
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
										return subDAO.updateNotificationFailure(potentialSub.getTenant(),
												potentialSub.getId(),
												SerializationTools.notifiedAt_formatter.format(LocalDateTime
														.ofInstant(Instant.ofEpochMilli(now), ZoneId.of("Z"))));
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
				});

	}

	private boolean notificationTriggerCheck(Subscription subscription, int triggerReason) {
		if (subscription.getTimeInterval() > 0) {
			return true;
		}
		Set<String> notificationTriggers = subscription.getNotificationTrigger();
		switch (triggerReason) {
		case AppConstants.BATCH_CREATE_REQUEST:
		case AppConstants.BATCH_UPSERT_REQUEST:
		case AppConstants.CREATE_REQUEST:
			return notificationTriggers.contains(NGSIConstants.NGSI_LD_NOTIFICATION_TRIGGER_ENTITY_CREATED)
					|| notificationTriggers.contains(NGSIConstants.NGSI_LD_NOTIFICATION_TRIGGER_ATTRIBUTE_CREATED);
		case AppConstants.BATCH_MERGE_REQUEST:
		case AppConstants.BATCH_UPDATE_REQUEST:
		case AppConstants.APPEND_REQUEST:
		case AppConstants.UPDATE_REQUEST:
		case AppConstants.MERGE_PATCH_REQUEST:
		case AppConstants.REPLACE_ENTITY_REQUEST:
		case AppConstants.REPLACE_ATTRIBUTE_REQUEST:
		case AppConstants.PARTIAL_UPDATE_REQUEST:
			return notificationTriggers.contains(NGSIConstants.NGSI_LD_NOTIFICATION_TRIGGER_ENTITY_UPDATED)
					|| notificationTriggers.contains(NGSIConstants.NGSI_LD_NOTIFICATION_TRIGGER_ATTRIBUTE_UPDATED)
					|| notificationTriggers.contains(NGSIConstants.NGSI_LD_NOTIFICATION_TRIGGER_ATTRIBUTE_CREATED);
		case AppConstants.DELETE_REQUEST:
		case AppConstants.BATCH_DELETE_REQUEST:
			return notificationTriggers.contains(NGSIConstants.NGSI_LD_NOTIFICATION_TRIGGER_ENTITY_DELETED);
		case AppConstants.DELETE_ATTRIBUTE_REQUEST:
			return notificationTriggers.contains(NGSIConstants.NGSI_LD_NOTIFICATION_TRIGGER_ENTITY_UPDATED)
					|| notificationTriggers.contains(NGSIConstants.NGSI_LD_NOTIFICATION_TRIGGER_ATTRIBUTE_DELETED);
		case AppConstants.UPSERT_REQUEST:
			return notificationTriggers.contains(NGSIConstants.NGSI_LD_NOTIFICATION_TRIGGER_ENTITY_CREATED)
					|| notificationTriggers.contains(NGSIConstants.NGSI_LD_NOTIFICATION_TRIGGER_ENTITY_UPDATED)
					|| notificationTriggers.contains(NGSIConstants.NGSI_LD_NOTIFICATION_TRIGGER_ATTRIBUTE_UPDATED)
					|| notificationTriggers.contains(NGSIConstants.NGSI_LD_NOTIFICATION_TRIGGER_ATTRIBUTE_CREATED);
		default:
			return false;
		}

	}

	private Uni<MqttClient> getMqttClient(NotificationParam notificationParam) {
		URI host = notificationParam.getEndPoint().getUri();
		String hostString = host.getUserInfo() + host.getHost() + host.getPort();
		MqttClient client;
		if (!host2MqttClient.containsKey(hostString)) {
			if (host.getUserInfo() != null) {
				String[] usrPass = host.getUserInfo().split(":");
				client = MqttClient.create(vertx,
						new MqttClientOptions().setUsername(usrPass[0]).setPassword(usrPass[1]));
			} else {
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
			if (sub.getNotification().getLastNotification() + sub.getTimeInterval() * 1000 < now) {
				sub.getNotification().setLastNotification(now);
				unis.add(queryFromSubscription(request, request.getTenant(), null, Maps.newHashMap()).onItem()
						.transformToUni(queryResult -> {
							if (queryResult.isEmpty()) {
								return Uni.createFrom().voidItem();
							}
							try {
								return sendNotification(request, queryResult);
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

	private Uni<List<Map<String, Object>>> queryFromSubscription(SubscriptionRequest request, String tenant,
			Set<String> idsTBU, Map<String, List<Map<String, Object>>> prevPayloadToUse) {
		HttpRequest<Buffer> req = webClient.getAbs(entityServiceUrl + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT);
		Map<String, String> queryParams = request.getAsQueryParams(idsTBU == null);
		for (Entry<String, String> entry : queryParams.entrySet()) {
			req = req.addQueryParam(entry.getKey(), entry.getValue());
		}
		if (idsTBU == null) {
			req = req.addQueryParam(NGSIConstants.ID, StringUtils.join(idsTBU, ','));
		}
		req = req.addQueryParam(NGSIConstants.QUERY_PARAMETER_DO_NOT_COMPACT, "true");
		req = req.putHeader(NGSIConstants.TENANT_HEADER, tenant).putHeader(HttpHeaders.ACCEPT,
				AppConstants.NGB_APPLICATION_JSONLD);

		return req.send().onItem().transform(resp -> {
			if (resp != null && resp.statusCode() == 200) {
				JsonArray jsonArray = resp.bodyAsJsonArray();
				List<Map<String, Object>> dataToNotify = new ArrayList<>(jsonArray.size());

				if (jsonArray.isEmpty()) {
					prevPayloadToUse.forEach((id, entities) -> {
						entities.forEach(entity -> {
							Map<String, Object> dupl = MicroServiceUtils.deepCopyMap(entity);
							dataToNotify.add(compareMaps(null, dupl));
						});
					});
				} else {
					jsonArray.forEach(entityObj -> {
						Map<String, Object> entity = (Map<String, Object>) entityObj;
						String entityId = (String) entity.get(NGSIConstants.JSON_LD_ID);
						Map<String, Object> prev;
						List<Map<String, Object>> entitiesPrev = prevPayloadToUse.get(entityId);
						if (entitiesPrev != null && !entitiesPrev.isEmpty()) {
							prev = entitiesPrev.get(0);
						} else {
							prev = null;
						}

						dataToNotify.add(compareMaps(entity, prev));
					});
				}
				return dataToNotify;
			}
			return null;
		});

	}

	public Uni<Void> handleRegistryNotification(InternalNotification message) {
		if (NGSIConstants.SUBSCRIPTION_NO_LONGER_MATCHING
				.equals(message.getPayload().get(NGSIConstants.NGSI_LD_TRIGGER_REASON_SHORT))) {
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

			return ldService.compact(remoteRequest.getPayload(), null, remoteRequest.getContext(), HttpUtils.opts, -1)
					.onItem().transformToUni(compacted -> {
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
									.putHeaders(SubscriptionTools.getHeaders(
											remoteRequest.getSubscription().getNotification(),
											remoteRequest.getSubscription().getOtherHead()))
									.sendJsonObject(new JsonObject(compacted)).onFailure().retry().atMost(3).onItem()
									.transformToUni(response -> {
										if (response.statusCode() >= 200 && response.statusCode() < 300) {
											String locationHeader = response.headers().get(HttpHeaders.LOCATION);
											// check if it's a relative path
											if (locationHeader.charAt(0) == '/') {
												locationHeader = remoteEndpoint + locationHeader;
											}
											internalSubId2ExternalEndpoint
													.put(subscriptionRequest.getSubscription().getId(), locationHeader);
										}
										return Uni.createFrom().voidItem();
									}).onFailure().recoverWithUni(t -> {
										logger.error("Failed to subscribe to remote host " + temp.toString(), t);
										return Uni.createFrom().voidItem();
									});
						} else {
							return Uni.createFrom().voidItem();
						}
					});
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
							return sendNotification(subscription, List.of(entity));
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

	public void addSyncService(SyncService subscriptionSyncService) {
		this.subscriptionSyncService = subscriptionSyncService;

	}

	public void reloadSubscription(String tenant, String id) {
		subDAO.loadSubscription(tenant, id).onItem().transformToUni(t -> {
			return ldService.parsePure(t.getItem2().get(NGSIConstants.JSON_LD_CONTEXT)).onItem().transformToUni(ctx -> {
				SubscriptionRequest request;
				try {
					request = new SubscriptionRequest(tenant, t.getItem1(), ctx);
				} catch (ResponseException e) {
					logger.error("Failed to reload subscription " + id);
					return Uni.createFrom().voidItem();
				}
				request.setSendTimestamp(-1);
				if (isIntervalSub(request)) {
					this.tenant2subscriptionId2IntervalSubscription.put(request.getTenant(), request.getId(), request);
				} else {
					this.tenant2subscriptionId2Subscription.put(request.getTenant(), request.getId(), request);
				}
				subscriptionId2RequestGlobal.put(request.getId(), request);
				return Uni.createFrom().voidItem();
			});
		}).subscribe().with(i -> {
			logger.info("Reloaded subscription: " + id);
		});
	}

	public Uni<Void> checkSubscriptionsForCSource(CSourceBaseRequest message) {
		Collection<SubscriptionRequest> potentialSubs = tenant2subscriptionId2Subscription.column(message.getTenant())
				.values();
		List<Uni<Void>> unis = Lists.newArrayList();
		for (SubscriptionRequest potentialSub : potentialSubs) {
			switch (message.getRequestType()) {
			case AppConstants.UPDATE_REQUEST:
				if (shouldFireReg(message.getPayload(), potentialSub)) {
					unis.add(subDAO.getRegById(message.getTenant(), message.getId()).onItem().transformToUni(rows -> {

						return SubscriptionTools.generateCsourceNotification(potentialSub,
								rows.iterator().next().getJsonObject(0).getMap(), message.getRequestType(), ldService)
								.onItem().transformToUni(notification -> {
									NotificationParam notificationParam = potentialSub.getSubscription()
											.getNotification();
									return handleRegistryNotification(new InternalNotification(potentialSub.getTenant(),
											potentialSub.getId(), notification));
								});
					}));
				}
				break;
			case AppConstants.CREATE_REQUEST:
			case AppConstants.DELETE_REQUEST:
				unis.add(SubscriptionTools.generateCsourceNotification(potentialSub, message.getPayload(),
						message.getRequestType(), ldService).onItem().transformToUni(notification -> {
							NotificationParam notificationParam = potentialSub.getSubscription().getNotification();
							return handleRegistryNotification(new InternalNotification(potentialSub.getTenant(),
									potentialSub.getId(), notification));
						}));
			default:
				break;
			}

		}
		if (unis.isEmpty()) {
			return Uni.createFrom().voidItem();
		}
		return Uni.combine().all().unis(unis).discardItems();
	}

	protected boolean shouldFireReg(Map<String, Object> entry, SubscriptionRequest subscription) {
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

}
