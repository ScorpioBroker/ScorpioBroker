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
import eu.neclab.ngsildbroker.commons.datatypes.QueryRemoteHost;
import eu.neclab.ngsildbroker.commons.datatypes.RegistrationEntry;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.SubscriptionRemoteHost;
import eu.neclab.ngsildbroker.commons.datatypes.ViaHeaders;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CSourceBaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.DeleteSubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.InternalNotification;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.UpdateSubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.results.CRUDSuccess;
import eu.neclab.ngsildbroker.commons.datatypes.results.NGSILDOperationResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.datatypes.terms.OmitTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.PickTerm;
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
import io.smallrye.mutiny.tuples.Tuple4;
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
@SuppressWarnings("unchecked")
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

	private Table<String, SubscriptionRemoteHost, Set<String>> tenant2RemoteHost2SubIds = HashBasedTable.create();

	private Table<String, String, Set<SubscriptionRemoteHost>> tenant2SubIds2RemoteHosts = HashBasedTable.create();

	private Table<String, String, List<RegistrationEntry>> queryTenant2CId2RegEntries = HashBasedTable.create();
	private Table<String, String, List<RegistrationEntry>> subscriptionTenant2CId2RegEntries = HashBasedTable.create();

	private Table<String, String, SubscriptionRequest> tenant2subscriptionId2Subscription = HashBasedTable.create();
	private Table<String, String, SubscriptionRequest> tenant2subscriptionId2IntervalSubscription = HashBasedTable
			.create();
	private Map<String, SubscriptionRequest> subscriptionId2RequestGlobal = Maps.newHashMap();
	private HashMap<String, List<SubscriptionRequest>> remoteNotifyCallbackId2SubRequest = new HashMap<String, List<SubscriptionRequest>>();
	private HashMap<SubscriptionRemoteHost, String> subRemoteRequest2RemoteNotifyCallbackId = new HashMap<SubscriptionRemoteHost, String>();
	private HashMap<String, Set<SubscriptionRemoteHost>> cId2RemoteHost = new HashMap<>();

	private WebClient webClient;

	private Map<String, MqttClient> host2MqttClient = Maps.newHashMap();
	private SyncService subscriptionSyncService = null;
	@ConfigProperty(name = "scorpio.at-context-server", defaultValue = "http://localhost:9090")
	private String atContextUrl;

	public Uni<Void> handleRegistryChange(CSourceBaseRequest req) {
		return RegistrationEntry.fromRegPayload(req.getPayload(), ldService).onItem().transformToUni(regs -> {
			List<RegistrationEntry> queryNewRegs = Lists.newArrayList();
			List<RegistrationEntry> subscriptionNewRegs = Lists.newArrayList();
			queryTenant2CId2RegEntries.remove(req.getTenant(), req.getId());
			subscriptionTenant2CId2RegEntries.remove(req.getTenant(), req.getId());
			if (req.getRequestType() != AppConstants.DELETE_REQUEST) {
				for (RegistrationEntry regEntry : regs) {
					if (regEntry.retrieveEntity() || regEntry.queryEntity() || regEntry.queryBatch()) {
						queryNewRegs.add(regEntry);
					}
					if (regEntry.createSubscription()) {
						subscriptionNewRegs.add(regEntry);
					}
				}
				queryTenant2CId2RegEntries.put(req.getTenant(), req.getId(), queryNewRegs);
				subscriptionTenant2CId2RegEntries.put(req.getTenant(), req.getId(), subscriptionNewRegs);
			}
			return recheckAllSubscriptionsForRegChange(req, subscriptionNewRegs);
		});
	}

	private Uni<Void> recheckAllSubscriptionsForRegChange(CSourceBaseRequest req,
			Collection<RegistrationEntry> regEntries) {
		List<Uni<Void>> unis = Lists.newArrayList();
		String tenant = req.getTenant();
		if (req.getRequestType() == AppConstants.DELETE_REQUEST) {
			Set<SubscriptionRemoteHost> remoteHosts = cId2RemoteHost.remove(req.getId());
			if (remoteHosts != null) {
				remoteHosts.forEach(host -> {

					Set<String> subIds = tenant2RemoteHost2SubIds.remove(tenant, host);
					if (subIds != null) {
						subIds.forEach(subId -> {
							Set<SubscriptionRemoteHost> tmp = tenant2SubIds2RemoteHosts.get(tenant, subId);
							tmp.remove(host);
							if (tmp.isEmpty()) {
								tenant2SubIds2RemoteHosts.remove(tenant, subId);
							}
						});
					}
					String callbackId = subRemoteRequest2RemoteNotifyCallbackId.remove(host);
					if (callbackId != null) {
						remoteNotifyCallbackId2SubRequest.remove(callbackId);
					}
					unis.add(SubscriptionTools.unsubsribeRemote(host, webClient));

				});
			}
		} else {
			Set<SubscriptionRemoteHost> remoteHosts = cId2RemoteHost.remove(req.getId());
			if (remoteHosts != null) {
				remoteHosts.forEach(host -> {
					Set<String> subIds = tenant2RemoteHost2SubIds.get(tenant, host);
					subIds.forEach(subId -> {
						SubscriptionRequest sub = tenant2subscriptionId2Subscription.get(tenant, subId);
						unis.add(updateRemoteSubs(sub, remoteHosts));
					});
				});
			}

		}
		if (unis.isEmpty()) {
			return Uni.createFrom().voidItem();
		}
		return Uni.combine().all().unis(unis).withUni(l -> Uni.createFrom().voidItem());
	}

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
		Uni<Void> loadSubs = subDAO.loadSubscriptions().onItem().transformToUni(subs -> {
			List<Uni<Tuple4<String, Map<String, Object>, String, Context>>> unis = Lists.newArrayList();
			subs.forEach(tuple -> {
				unis.add(ldService.parsePure(tuple.getItem4().get(NGSIConstants.JSON_LD_CONTEXT)).onItem()
						.transform(ctx -> {
							return Tuple4.of(tuple.getItem1(), tuple.getItem2(), tuple.getItem3(), ctx);
						}));
			});
			if (unis.isEmpty()) {
				return Uni.createFrom().voidItem();
			}
			return Uni.combine().all().unis(unis).with(list -> {
				for (Object obj : list) {
					Tuple4<String, Map<String, Object>, String, Context> tuple = (Tuple4<String, Map<String, Object>, String, Context>) obj;
					SubscriptionRequest request;

					try {
						request = new SubscriptionRequest(tuple.getItem1(), tuple.getItem2(), tuple.getItem4());
						request.setContextId(tuple.getItem3());
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

		});

		Uni<Void> loadRegs = subDAO.getAllRegistries().onItem().transformToUni(regs -> {
			regs.cellSet().forEach(cell -> {
				List<RegistrationEntry> value = cell.getValue();
				String rowKey = cell.getRowKey();
				String columnKey = cell.getColumnKey();
				List<RegistrationEntry> tmpQuery = new ArrayList<>(value.size());
				List<RegistrationEntry> tmpSub = new ArrayList<>(value.size());
				value.forEach(regEntry -> {
					if (regEntry.retrieveEntity() || regEntry.queryBatch() || regEntry.queryEntity()) {
						tmpQuery.add(regEntry);
					}
					if (regEntry.createSubscription()) {
						tmpSub.add(regEntry);
					}
				});
				if (!tmpQuery.isEmpty()) {
					queryTenant2CId2RegEntries.put(rowKey, columnKey, tmpQuery);
				}
				if (!tmpSub.isEmpty()) {
					subscriptionTenant2CId2RegEntries.put(rowKey, columnKey, tmpSub);
				}
			});
			return Uni.createFrom().voidItem();
		});
		Uni.combine().all().unis(loadSubs, loadRegs).with(l -> l).await().indefinitely();

	}

	private boolean isIntervalSub(SubscriptionRequest request) {
		return request.getSubscription().getTimeInterval() > 0;
	}

	public Uni<NGSILDOperationResult> createSubscription(HeadersMultiMap linkHead, String tenant,
			Map<String, Object> subscription, Context contextLink, ViaHeaders viaHeaders) {
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
			request.setContextId(contextId);
			return subDAO.createSubscription(request, contextId).onItem().transformToUni(t -> {
				if (isIntervalSub(request)) {
					this.tenant2subscriptionId2IntervalSubscription.put(request.getTenant(), request.getId(), request);

				} else {
					tenant2subscriptionId2Subscription.put(tenant, request.getId(), request);
				}
				subscriptionId2RequestGlobal.put(request.getId(), request);
				Uni<Void> syncService;
				if (subscriptionSyncService != null) {
					logger.debug("sync service");
					syncService = subscriptionSyncService.sync(request);
				} else {
					logger.debug("No sync service");
					syncService = Uni.createFrom().voidItem();
				}

				return syncService.onItem().transformToUni(v2 -> {
					return updateRemoteSubs(request, viaHeaders).onItem().transform(v3 -> {
						NGSILDOperationResult result = new NGSILDOperationResult(
								AppConstants.CREATE_SUBSCRIPTION_REQUEST, request.getId());
						result.addSuccess(new CRUDSuccess(null, null, request.getId(), Sets.newHashSet()));
						return result;
					});

				});
			}).onFailure().recoverWithUni(e -> {
				if (e instanceof PgException pge && pge.getSqlState().equals(AppConstants.SQL_ALREADY_EXISTS)) {
					return Uni.createFrom().failure(new ResponseException(ErrorType.AlreadyExists,
							"Subscription with id " + request.getId() + " exists"));
				} else {
					return Uni.createFrom().failure(new ResponseException(ErrorType.InternalError, e.getMessage()));
				}

			});
		});

	}

	private Uni<Void> updateRemoteSubs(SubscriptionRequest request, ViaHeaders viaHeaders) {
		Subscription sub = request.getSubscription();

		Collection<SubscriptionRemoteHost> remoteHosts = SubscriptionTools.getRemoteSubscriptions(sub,
				sub.getEntities(), sub.getNotification().getAttrs(), sub.getLdQuery(), sub.getLdGeoQuery(),
				sub.getScopeQuery(), sub.getLanguageQuery(),
				subscriptionTenant2CId2RegEntries.row(request.getTenant()).values(), request.getContext(), viaHeaders);
		return updateRemoteSubs(request, remoteHosts);
	}

	private Uni<Void> updateRemoteSubs(SubscriptionRequest request, Collection<SubscriptionRemoteHost> remoteHosts) {
		Subscription sub = request.getSubscription();

		if (remoteHosts.isEmpty()) {
			return Uni.createFrom().voidItem();
		}
		String tenant = request.getTenant();
		String subId = request.getSubscription().getId();
		Set<SubscriptionRemoteHost> toStore = Sets.newHashSet();
		remoteHosts.forEach(remoteHost -> {
			Set<String> existingSubIds = tenant2RemoteHost2SubIds.get(tenant, remoteHosts);
			Set<SubscriptionRemoteHost> existingRemoteHosts = tenant2SubIds2RemoteHosts.get(tenant, subId);

			if (existingSubIds == null) {
				existingSubIds = Sets.newHashSet();
				tenant2RemoteHost2SubIds.put(tenant, remoteHost, existingSubIds);
			}
			if (existingRemoteHosts == null) {
				existingRemoteHosts = Sets.newHashSet();
				tenant2SubIds2RemoteHosts.put(tenant, subId, existingRemoteHosts);
			}
			if (!existingSubIds.contains(subId)) {
				existingSubIds.add(subId);
				toStore.add(remoteHost);
			}
			existingRemoteHosts.remove(remoteHost);
			Set<SubscriptionRemoteHost> rHosts = cId2RemoteHost.get(remoteHost.cSourceId());
			if (rHosts == null) {
				rHosts = Sets.newHashSet();
				cId2RemoteHost.put(remoteHost.cSourceId(), rHosts);
			}
			rHosts.add(remoteHost);
		});
		List<Uni<Void>> unis = Lists.newArrayList();
		unis.add(unsubsribeRemote(tenant2SubIds2RemoteHosts.get(tenant, subId), subId));
		unis.add(subscribeRemote(toStore, request));

		return Uni.combine().all().unis(unis).withUni(l -> {
			tenant2SubIds2RemoteHosts.get(tenant, subId).addAll(toStore);
			return Uni.createFrom().voidItem();
		});
	}

	private Uni<Void> subscribeRemote(Set<SubscriptionRemoteHost> subs, SubscriptionRequest req) {
		if (subs.isEmpty()) {
			return Uni.createFrom().voidItem();
		}
		List<Uni<Void>> unis = new ArrayList<>(subs.size());
		String gateway = microServiceUtils.getGatewayURL().toString() + "/remotenotify/";
		subs.forEach(sub -> {
			String endpoint = prepareNotificationServlet(req, sub);
			unis.add(SubscriptionTools.subsribeRemote(sub, webClient, gateway + endpoint));

		});
		return Uni.combine().all().unis(unis).withUni(l -> Uni.createFrom().voidItem());
	}

	private String prepareNotificationServlet(SubscriptionRequest req, SubscriptionRemoteHost remoteHost) {
		String uuid = Long.toString(UUID.randomUUID().getLeastSignificantBits());
		List<SubscriptionRequest> reqList = Lists.newArrayList(req);
		remoteNotifyCallbackId2SubRequest.put(uuid, reqList);
		subRemoteRequest2RemoteNotifyCallbackId.put(remoteHost, uuid);
		return uuid;

	}

	private Uni<Void> unsubsribeRemote(Set<SubscriptionRemoteHost> subs, String subId) {
		if (subs.isEmpty()) {
			return Uni.createFrom().voidItem();
		}
		List<Uni<Void>> unis = new ArrayList<>(subs.size());
		subs.forEach(sub -> {
			String tenant = sub.tenant();
			Set<String> activeSubsForRemote = tenant2RemoteHost2SubIds.get(tenant, subId);
			activeSubsForRemote.remove(subId);
			if (activeSubsForRemote.isEmpty()) {
				tenant2RemoteHost2SubIds.remove(tenant, subId);
				unis.add(SubscriptionTools.unsubsribeRemote(sub, webClient));
			}
		});
		return Uni.combine().all().unis(unis).withUni(l -> Uni.createFrom().voidItem());
	}

	public Uni<NGSILDOperationResult> updateSubscription(String tenant, String subscriptionId,
			Map<String, Object> update, Context context, ViaHeaders viaHeaders) {
		UpdateSubscriptionRequest request = new UpdateSubscriptionRequest(tenant, subscriptionId, update, context);
		return localContextService.createImplicitly(tenant, request.getContext().serialize()).onItem()
				.transformToUni(contextId -> {
					request.setContextId(contextId);
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
							return syncService.onItem().transformToUni(v2 -> {
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
								return updateRemoteSubs(request, viaHeaders).onItem().transform(v3 -> {
									return new NGSILDOperationResult(AppConstants.UPDATE_SUBSCRIPTION_REQUEST,
											request.getId());
								});
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
				return unsubscribeRemote(tenant, subscriptionId).onItem().transform(v -> {
//				try {
//					MicroServiceUtils.serializeAndSplitObjectAndEmit(request, messageSize, internalSubEmitter,
//							objectMapper);
//				} catch (ResponseException e) {
//					logger.error("Failed to serialize subscription message", e);
//				}
					return new NGSILDOperationResult(AppConstants.DELETE_SUBSCRIPTION_REQUEST, request.getId());
				});
			});
		});
	}

	private Uni<Void> unsubscribeRemote(String tenant, String subscriptionId) {
		Set<SubscriptionRemoteHost> remoteHosts = tenant2SubIds2RemoteHosts.remove(tenant, subscriptionId);
		// Set<String> subIds = tenant2RemoteHost2SubIds.remove(tenant, host);
		List<Uni<Void>> unis = Lists.newArrayList();
		if (remoteHosts != null) {
			remoteHosts.forEach(remoteHost -> {
				Set<String> tmp = tenant2RemoteHost2SubIds.get(tenant, remoteHost);
				tmp.remove(subscriptionId);
				if (tmp.isEmpty()) {
					tenant2RemoteHost2SubIds.remove(tenant, remoteHost);
				}
				String callbackId = subRemoteRequest2RemoteNotifyCallbackId.remove(remoteHost);
				if (callbackId != null) {
					remoteNotifyCallbackId2SubRequest.remove(callbackId);
				}
				unis.add(SubscriptionTools.unsubsribeRemote(remoteHost, webClient));
			});
		}
		if(unis.isEmpty()) {
			return Uni.createFrom().voidItem();
		}
		return Uni.combine().all().unis(unis).withUni(l -> Uni.createFrom().voidItem());
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
		return checkSubscriptions(message, potentialSubs);
	}

	public Uni<Void> checkSubscriptions(BaseRequest message, Collection<SubscriptionRequest> potentialSubs) {

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
				unis.add(
						queryFromSubscription(potentialSub, message.getTenant(), idsTbu, prevPayloadToUse, payloadToUse)
								.onItem().transformToUni(tbs -> {
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
													toAddLater.add(
															compareMaps(prevPayloadToUse.get(entityId).get(0), entity));
												}
											} else {
												it.remove();
												for (int i = 0; i < payload.size(); i++) {
													Map<String, Object> pEntry = payload.get(0);
													Map<String, Object> dupl = MicroServiceUtils.deepCopyMap(entity);
													dupl.putAll(pEntry);
													if (potentialSub.getSubscription().getNotification()
															.getShowChanges()) {
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
					dataToSend = mergePrevAndNew(payloadToUse, null,
							potentialSub.getSubscription().getNotification().getShowChanges());
					break;
				}
				case AppConstants.BATCH_UPDATE_REQUEST:
				case AppConstants.BATCH_UPSERT_REQUEST:
				case AppConstants.APPEND_REQUEST:
				case AppConstants.UPDATE_REQUEST:
				case AppConstants.BATCH_MERGE_REQUEST:
				case AppConstants.REPLACE_ATTRIBUTE_REQUEST:
				case AppConstants.MERGE_PATCH_REQUEST:
				case AppConstants.PARTIAL_UPDATE_REQUEST: {
					dataToSend = mergePrevAndNew(payloadToUse, prevPayloadToUse,
							potentialSub.getSubscription().getNotification().getShowChanges());
					break;
				}
				case AppConstants.REPLACE_ENTITY_REQUEST: {
					dataToSend = mergePrevAndNewReplace(payloadToUse, prevPayloadToUse,
							potentialSub.getSubscription().getNotification().getShowChanges());
					break;
				}
				case AppConstants.DELETE_ATTRIBUTE_REQUEST: {
					List<Map<String, Object>> tmp = Lists.newArrayList();
					if (potentialSub.getSubscription().getNotification().getShowChanges()) {
						prevPayloadToUse.values().forEach(payloads -> {
							payloads.forEach(payload -> {
								List<Map<String, Object>> attribs = (List<Map<String, Object>>) payload
										.get(message.getAttribName());
								if (attribs != null) {
									for (Map<String, Object> attrib : attribs) {
										putOldAttribFromDelete(attrib, message.getSendTimestamp());
									}
									tmp.add(payload);
								}
							});
						});
					} else {
						prevPayloadToUse.values().forEach(payloads -> {
							payloads.forEach(payload -> {
								payload.remove(message.getAttribName());
								tmp.add(payload);
							});
						});
					}
					dataToSend = tmp;
					break;
				}
				case AppConstants.DELETE_REQUEST:
				case AppConstants.BATCH_DELETE_REQUEST: {
					List<Map<String, Object>> tmp = Lists.newArrayList();
					prevPayloadToUse.values().forEach(payloads -> {
						payloads.forEach(payload -> {
							payload.put(NGSIConstants.NGSI_LD_DELETED_AT,
									List.of(Map.of(NGSIConstants.JSON_LD_TYPE, NGSIConstants.NGSI_LD_DATE_TIME,
											NGSIConstants.JSON_LD_VALUE,
											SerializationTools.toDateTimeString(message.getSendTimestamp()))));
							tmp.add(payload);
						});
					});
					dataToSend = tmp;
					break;
				}
				default:
					break;
				}

			}
			Iterator<Map<String, Object>> it = dataToSend.iterator();
			while (it.hasNext()) {
				Map<String, Object> next = it.next();
				if (!potentialSub.fullEntityCheckToSendOut((String) next.get(NGSIConstants.JSON_LD_ID), next,
						ALL_TYPES_SUB, null)) {
					it.remove();
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
				addNoOldValues(result, newValues, showChanges);

			} else {
				List<Map<String, Object>> oldValues = prevPayloadToUse.get(entityId);
				if (oldValues == null) {
					result.addAll(newValues);
				} else {
					for (int i = 0; i < newValues.size(); i++) {
						Map<String, Object> newValue = newValues.get(i);
						if (i >= oldValues.size()) {
							addNoOldValue(result, newValue, showChanges);
						} else {
							Map<String, Object> oldValue = oldValues.get(i);
							if (oldValue == null) {
								addNoOldValue(result, newValue, showChanges);
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

	private List<Map<String, Object>> mergePrevAndNewReplace(Map<String, List<Map<String, Object>>> payloadToUse,
			Map<String, List<Map<String, Object>>> prevPayloadToUse, Boolean showChanges) {
		List<Map<String, Object>> result = Lists.newArrayList();
		for (Entry<String, List<Map<String, Object>>> entry : payloadToUse.entrySet()) {
			String entityId = entry.getKey();
			List<Map<String, Object>> newValues = entry.getValue();
			if (prevPayloadToUse == null) {
				addNoOldValues(result, newValues, showChanges);

			} else {
				List<Map<String, Object>> oldValues = prevPayloadToUse.get(entityId);
				if (oldValues == null) {
					result.addAll(newValues);
				} else {
					for (int i = 0; i < newValues.size(); i++) {
						Map<String, Object> newValue = newValues.get(i);
						if (i >= oldValues.size()) {
							addNoOldValue(result, newValue, showChanges);
						} else {
							Map<String, Object> oldValue = oldValues.get(i);
							if (oldValue == null) {
								addNoOldValue(result, newValue, showChanges);
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

	private void addNoOldValues(List<Map<String, Object>> result, List<Map<String, Object>> newValues,
			Boolean showChanges) {
		if (showChanges) {
			for (Map<String, Object> newValue : newValues) {
				addNoOldValue(result, newValue, showChanges);
			}
		} else {
			result.addAll(newValues);
		}

	}

	private void addNoOldValue(List<Map<String, Object>> result, Map<String, Object> newValue, Boolean showChanges) {
		if (showChanges) {
			for (Entry<String, Object> entry : newValue.entrySet()) {
				switch (entry.getKey()) {
				case NGSIConstants.JSON_LD_ID:
				case NGSIConstants.JSON_LD_TYPE:
				case NGSIConstants.NGSI_LD_SCOPE:
				case NGSIConstants.NGSI_LD_MODIFIED_AT:
				case NGSIConstants.NGSI_LD_CREATED_AT:
					continue;
				default: {
					List<Map<String, Object>> attribEntries = (List<Map<String, Object>>) entry.getValue();
					for (Map<String, Object> attribEntry : attribEntries) {
						mergeAttribToNone(attribEntry);
					}

				}
				}
			}
		}
		result.add(newValue);

	}

	private Map<String, Object> mergePrevAndNewEntity(Map<String, Object> oldValue, Map<String, Object> newValue,
			Boolean showChanges) {
		Map<String, Object> result = MicroServiceUtils.deepCopyMap(oldValue);
		for (Entry<String, Object> entry : newValue.entrySet()) {
			String attribName = entry.getKey();
			switch (attribName) {
			case NGSIConstants.JSON_LD_ID:
				result.put(attribName, entry.getValue());
				break;
			case NGSIConstants.JSON_LD_TYPE:
				List<String> oldTypes = (List<String>) result.get(NGSIConstants.JSON_LD_TYPE);
				if (oldTypes == null) {
					result.put(attribName, entry.getValue());
				} else {
					List<String> mergedTypes = Stream
							.concat((oldTypes).stream(), ((List<String>) entry.getValue()).stream()).distinct()
							.collect(Collectors.toList());
					result.put(attribName, mergedTypes);
				}
				break;
			case NGSIConstants.NGSI_LD_SCOPE:
			case NGSIConstants.NGSI_LD_MODIFIED_AT:
				result.put(attribName, entry.getValue());
				break;
			case NGSIConstants.NGSI_LD_CREATED_AT:
				continue;
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
		newEntry.put(NGSIConstants.NGSI_LD_CREATED_AT, oldEntry.get(NGSIConstants.NGSI_LD_CREATED_AT));
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

	private void putOldAttribFromDelete(Map<String, Object> oldEntry, long timeStamp) {
		oldEntry.put(NGSIConstants.NGSI_LD_DELETED_AT,
				List.of(Map.of(NGSIConstants.JSON_LD_TYPE, NGSIConstants.NGSI_LD_DATE_TIME, NGSIConstants.JSON_LD_VALUE,
						SerializationTools.toDateTimeString(timeStamp))));
		if (oldEntry.containsKey(NGSIConstants.NGSI_LD_HAS_VALUE)) {
			oldEntry.put(NGSIConstants.PREVIOUS_VALUE, oldEntry.remove(NGSIConstants.NGSI_LD_HAS_VALUE));
		} else if (oldEntry.containsKey(NGSIConstants.NGSI_LD_HAS_OBJECT)) {
			oldEntry.put(NGSIConstants.PREVIOUS_OBJECT, oldEntry.remove(NGSIConstants.NGSI_LD_HAS_OBJECT));
		} else if (oldEntry.containsKey(NGSIConstants.NGSI_LD_HAS_JSON)) {
			oldEntry.put(NGSIConstants.PREVIOUS_JSON, oldEntry.remove(NGSIConstants.NGSI_LD_HAS_JSON));
		} else if (oldEntry.containsKey(NGSIConstants.NGSI_LD_HAS_LANGUAGE_MAP)) {
			oldEntry.put(NGSIConstants.PREVIOUS_LANGUAGE_MAP, oldEntry.remove(NGSIConstants.NGSI_LD_HAS_LANGUAGE_MAP));
		} else if (oldEntry.containsKey(NGSIConstants.NGSI_LD_HAS_OBJECT_LIST)) {
			oldEntry.put(NGSIConstants.PREVIOUS_OJBECT_LIST, oldEntry.remove(NGSIConstants.NGSI_LD_HAS_OBJECT_LIST));
		} else if (oldEntry.containsKey(NGSIConstants.NGSI_LD_HAS_LIST)) {
			oldEntry.put(NGSIConstants.PREVIOUS_VALUE_LIST, oldEntry.remove(NGSIConstants.NGSI_LD_HAS_LIST));
		} else if (oldEntry.containsKey(NGSIConstants.NGSI_LD_HAS_VOCAB)) {
			oldEntry.put(NGSIConstants.PREVIOUS_VOCAB, oldEntry.remove(NGSIConstants.NGSI_LD_HAS_VOCAB));
		}

	}

	private Uni<Void> sendNotification(SubscriptionRequest potentialSub, List<Map<String, Object>> dataToSend) {
		if (dataToSend == null || dataToSend.isEmpty()) {
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
				unis.add(queryFromSubscription(request, request.getTenant(), null, Maps.newHashMap(), Maps.newHashMap())
						.onItem().transformToUni(queryResult -> {
							if (queryResult == null || queryResult.isEmpty()) {
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
		return Uni.combine().all().unis(unis).with(list -> list).onItem()
				.transformToUni(list -> Uni.createFrom().voidItem());
	}

	private Uni<List<Map<String, Object>>> queryFromSubscription(SubscriptionRequest request, String tenant,
			Set<String> idsTBU, Map<String, List<Map<String, Object>>> prevPayloadToUse,
			Map<String, List<Map<String, Object>>> payloadToUse) {
		HttpRequest<Buffer> req = webClient.postAbs(entityServiceUrl + NGSIConstants.ENDPOINT_BATCH_QUERY);
		Map<String, Object> queryBody = request.getAsQueryBody(idsTBU, atContextUrl);

		req = req.addQueryParam(NGSIConstants.QUERY_PARAMETER_DO_NOT_COMPACT, "true");
		req = req.addQueryParam(NGSIConstants.QUERY_PARAMETER_LIMIT, "1000");
		req = req.putHeader(NGSIConstants.TENANT_HEADER, tenant)
				.putHeader(HttpHeaders.ACCEPT, AppConstants.NGB_APPLICATION_JSON)
				.putHeader(HttpHeaders.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSONLD);
		String batchString;
		try {
			batchString = JsonUtils.toPrettyString(queryBody);
		} catch (Exception e) {
			logger.warn("failed to serialize batch request");
			return Uni.createFrom().item(Lists.newArrayList());
		}
		return req.sendBuffer(Buffer.buffer(batchString)).onItem().transform(resp -> {
			if (resp != null && resp.statusCode() == 200) {
				JsonArray jsonArray = resp.bodyAsJsonArray();
				jsonArray.forEach(entityObj -> {
					Map<String, Object> entity = ((JsonObject) entityObj).getMap();
					String entityId = (String) entity.get(NGSIConstants.JSON_LD_ID);
					List<Map<String, Object>> prevPayloadList = prevPayloadToUse.get(entityId);
					if (prevPayloadList != null) {
						for (Map<String, Object> prevPayload : prevPayloadList) {
							Map<String, Object> dupl = MicroServiceUtils.deepCopyMap(entity);
							mergePrevIntoQueryResult(prevPayload, dupl);
						}
					} else {
						List<Map<String, Object>> payloadList = payloadToUse.get(entityId);
						if (payloadList != null) {
							for (Map<String, Object> payload : payloadList) {
								Map<String, Object> dupl = MicroServiceUtils.deepCopyMap(entity);
								mergePrevIntoQueryResult(payload, dupl);
							}

						} else {
							payloadToUse.put(entityId, Lists.newArrayList(entity));
						}
					}
				});
				List<Map<String, Object>> dataToNotify = mergePrevAndNew(payloadToUse, prevPayloadToUse,
						request.getSubscription().getNotification().getShowChanges());
				PickTerm pick = request.getSubscription().getNotification().getPick();
				if (pick != null) {
					Iterator<Map<String, Object>> it = dataToNotify.iterator();
					while (it.hasNext()) {
						Map<String, Object> entity = it.next();
						if (!pick.calculateEntity(entity, false, null, null, true)) {
							it.remove();
						}
					}
				}
				OmitTerm omit = request.getSubscription().getNotification().getOmit();
				if (omit != null) {
					Iterator<Map<String, Object>> it = dataToNotify.iterator();
					while (it.hasNext()) {
						Map<String, Object> entity = it.next();
						if (!omit.calculateEntity(entity, false, null, null, true)) {
							it.remove();
						}
					}
				}
				return dataToNotify;
			}

			logger.error("unexpected result from subscription based query" + resp);

			return null;
		});

	}

	private void mergePrevIntoQueryResult(Map<String, Object> prevPayload, Map<String, Object> dupl) {
		for (Entry<String, Object> entry : prevPayload.entrySet()) {
			String attribName = entry.getKey();
			if (attribName.equals(NGSIConstants.JSON_LD_ID) || attribName.equals(NGSIConstants.JSON_LD_TYPE)
					|| attribName.equals(NGSIConstants.SCOPE)) {
				continue;
			}
			if (attribName.equals(NGSIConstants.NGSI_LD_MODIFIED_AT) || attribName.equals(NGSIConstants.CREATEDAT)
					|| attribName.equals(NGSIConstants.NGSI_LD_DELETED_AT)) {
				dupl.put(attribName, entry.getValue());
				continue;
			}
			mergeInPrevAttribIn(dupl, attribName, entry.getValue());

		}

	}

	private void mergeInPrevAttribIn(Map<String, Object> dupl, String attribName, Object value) {
		List<Map<String, Object>> newAttribValueList = (List<Map<String, Object>>) dupl.get(attribName);
		List<Map<String, Object>> prevAttribValueList = (List<Map<String, Object>>) value;
		for (Map<String, Object> prevAttribValue : prevAttribValueList) {
			Object prevDataSetId = prevAttribValue.get(NGSIConstants.NGSI_LD_DATA_SET_ID);
			Iterator<Map<String, Object>> it = newAttribValueList.iterator();
			List<Map<String, Object>> toAdd = Lists.newArrayList();
			while (it.hasNext()) {
				Map<String, Object> newAttrib = it.next();
				Object newDataSetId = newAttrib.get(NGSIConstants.NGSI_LD_DATA_SET_ID);
				if ((newDataSetId == null && prevDataSetId == null)
						|| (newDataSetId != null && prevDataSetId != null && newDataSetId.equals(prevDataSetId))) {
					it.remove();
					toAdd.add(newAttrib);
				}

			}
			newAttribValueList.addAll(toAdd);
		}

	}

	public Uni<Void> remoteNotify(String notificationEndpoint, Map<String, Object> notification, Context context) {
		List<SubscriptionRequest> subscriptions = remoteNotifyCallbackId2SubRequest.get(notificationEndpoint);
		if (subscriptions == null) {
			return Uni.createFrom().voidItem();
		}
		List<Map<String, Object>> data = (List<Map<String, Object>>) notification.get(NGSIConstants.NGSI_LD_DATA);

		List<Uni<Void>> unis = Lists.newArrayList();
		Map<String, List<Map<String, Object>>> prevValues = Maps.newHashMap();
		Map<String, List<Map<String, Object>>> newValues = Maps.newHashMap();
		Map<String, List<Map<String, Object>>> deletedEntities = Maps.newHashMap();

		Map<String, List<Map<String, Object>>> deletedPrevValues = Maps.newHashMap();
		Map<String, List<Map<String, Object>>> deletedNewValues = Maps.newHashMap();

		Set<String> ids = Sets.newHashSet();

		for (Map<String, Object> entry : data) {
			String id = (String) entry.get(NGSIConstants.JSON_LD_ID);
			ids.add(id);
			if (entry.containsKey(NGSIConstants.NGSI_LD_DELETED_AT)) {
				List<Map<String, Object>> tmp = deletedEntities.get(id);
				if (tmp == null) {
					tmp = Lists.newArrayList();
					deletedEntities.put(id, tmp);
				}
				tmp.add(entry);
				continue;
			}
			Map<String, Object> prevValue = new HashMap<>(entry.size());
			Iterator<Entry<String, Object>> it = prevValue.entrySet().iterator();
			while (it.hasNext()) {
				Entry<String, Object> attribEntry = it.next();
				Tuple2<Boolean, Object> prevT = createPreviousVariant(attribEntry);
				Object prev = prevT.getItem2();
				if (prev != null) {
					if (prevT.getItem1()) {
						//
					}
					prevValue.put(attribEntry.getKey(), prev);
				}
				if (attribEntry.getValue() instanceof List<?> l && l.isEmpty()) {
					it.remove();
				}
			}
			List<Map<String, Object>> tmp = prevValues.get(id);
			if (tmp == null) {
				tmp = Lists.newArrayList();
			}
			tmp.add(prevValue);

			tmp = newValues.get(id);
			if (tmp == null) {
				tmp = Lists.newArrayList();
			}
			tmp.add(entry);

		}
		BaseRequest baseReq = new BaseRequest();
		baseReq.setPrevPayload(prevValues);
		baseReq.setPayload(newValues);

		baseReq.setIds(ids);
		baseReq.setRequestType(AppConstants.UPDATE_REQUEST);
		baseReq.setDistributed(true);
		return checkSubscriptions(baseReq, subscriptions);
	}

	private Tuple2<Boolean, Object> createPreviousVariant(Entry<String, Object> attribEntry) {
		switch (attribEntry.getKey()) {
		case NGSIConstants.JSON_LD_ID:
		case NGSIConstants.JSON_LD_TYPE:
		case NGSIConstants.NGSI_LD_CREATED_AT:
		case NGSIConstants.NGSI_LD_MODIFIED_AT:
			return Tuple2.of(false, attribEntry.getValue());
		default: {
			if (attribEntry.getValue() instanceof List<?> l) {
				List<Map<String, Object>> result = new ArrayList<>(l.size());
				Iterator<?> it = l.iterator();
				boolean onlyDeletedAt = true;
				while (it.hasNext()) {
					Object obj = it.next();
					if (obj instanceof Map<?, ?> m) {
						boolean keep = m.remove(NGSIConstants.NGSI_LD_DELETED_AT) == null;
						onlyDeletedAt = onlyDeletedAt && !keep;
						Object prevValue = m.remove(NGSIConstants.PREVIOUS_VALUE);
						Map<String, Object> oldEntry = null;
						if (prevValue != null) {
							keep = keep && true;
							oldEntry = MicroServiceUtils.deepCopyMap((Map<String, Object>) m);
							oldEntry.put(NGSIConstants.NGSI_LD_HAS_VALUE, prevValue);
						} else {
							prevValue = m.remove(NGSIConstants.PREVIOUS_OBJECT);
							if (prevValue != null) {
								keep = keep && true;
								oldEntry = MicroServiceUtils.deepCopyMap((Map<String, Object>) m);
								oldEntry.put(NGSIConstants.NGSI_LD_HAS_OBJECT, prevValue);
							} else {
								prevValue = m.remove(NGSIConstants.PREVIOUS_JSON);
								if (prevValue != null) {
									keep = keep && true;
									oldEntry = MicroServiceUtils.deepCopyMap((Map<String, Object>) m);
									oldEntry.put(NGSIConstants.NGSI_LD_HAS_JSON, prevValue);
								} else {
									prevValue = m.remove(NGSIConstants.PREVIOUS_LANGUAGE_MAP);
									if (prevValue != null) {
										keep = keep && true;
										oldEntry = MicroServiceUtils.deepCopyMap((Map<String, Object>) m);
										oldEntry.put(NGSIConstants.NGSI_LD_HAS_LANGUAGE_MAP, prevValue);
									} else {
										prevValue = m.remove(NGSIConstants.PREVIOUS_OJBECT_LIST);
										if (prevValue != null) {
											keep = keep && true;
											oldEntry = MicroServiceUtils.deepCopyMap((Map<String, Object>) m);
											oldEntry.put(NGSIConstants.NGSI_LD_HAS_OBJECT_LIST, prevValue);
										} else {
											prevValue = m.remove(NGSIConstants.PREVIOUS_VALUE_LIST);
											if (prevValue != null) {
												keep = keep && true;
												oldEntry = MicroServiceUtils.deepCopyMap((Map<String, Object>) m);
												oldEntry.put(NGSIConstants.NGSI_LD_HAS_LIST, prevValue);
											} else {
												prevValue = m.remove(NGSIConstants.PREVIOUS_VOCAB);
												if (prevValue != null) {
													keep = keep && true;
													oldEntry = MicroServiceUtils.deepCopyMap((Map<String, Object>) m);
													oldEntry.put(NGSIConstants.NGSI_LD_HAS_VOCAB, prevValue);
												}
											}
										}
									}
								}
							}
						}
						if (!keep) {
							it.remove();
						}
						if (oldEntry != null) {
							result.add(oldEntry);
						}
					}
				}
				if (result.isEmpty()) {
					return Tuple2.of(onlyDeletedAt, null);
				} else {
					return Tuple2.of(onlyDeletedAt, result);
				}
			}
			return Tuple2.of(false, attribEntry.getValue());
		}
		}

	}

	@PreDestroy
	public void unsubscribeToAllRemote() {
		List<Uni<Void>> unis = new ArrayList<>(subRemoteRequest2RemoteNotifyCallbackId.size());
		for (SubscriptionRemoteHost entry : subRemoteRequest2RemoteNotifyCallbackId.keySet()) {
			logger.debug("Unsubscribing to remote host " + entry + " before shutdown");
			unis.add(SubscriptionTools.unsubsribeRemote(entry, webClient));
		}
		if (!unis.isEmpty()) {
			Uni.combine().all().unis(unis).discardItems().await().atMost(Duration.ofSeconds(30));
		}
	}

	public Uni<Void> syncDeleteSubscription(String tenant, String subId) {
		tenant2subscriptionId2IntervalSubscription.remove(tenant, subId);
		tenant2subscriptionId2Subscription.remove(tenant, subId);
		return Uni.createFrom().voidItem();
	}

	public Uni<Void> syncUpdateSubscription(String tenant, String subId) {
		return subDAO.getSubscription(tenant, subId).onFailure().recoverWithItem(e -> {
			tenant2subscriptionId2IntervalSubscription.remove(tenant, subId);
			tenant2subscriptionId2Subscription.remove(tenant, subId);
			return null;
		}).onItem().transformToUni(rows -> {
			if (rows == null || rows.size() == 0) {
				return Uni.createFrom().voidItem();
			}
			Row first = rows.iterator().next();
			return ldService.parsePure(first.getJsonObject(1).getMap()).onItem().transformToUni(ctx -> {
				SubscriptionRequest request;
				try {
					request = new SubscriptionRequest(tenant, first.getJsonObject(0).getMap(), ctx);
					request.setContextId(first.getString(2));
					request.getSubscription().addOtherHead(NGSIConstants.LINK_HEADER,
							"<%s>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\""
									.formatted(request.getSubscription().getJsonldContext()));
					request.getSubscription().addOtherHead(NGSIConstants.TENANT_HEADER, request.getTenant());
					request.setSendTimestamp(-1);
					if (isIntervalSub(request)) {
						tenant2subscriptionId2IntervalSubscription.put(request.getTenant(), request.getId(), request);
						tenant2subscriptionId2Subscription.remove(tenant, request.getId());
					} else {
						tenant2subscriptionId2Subscription.put(request.getTenant(), request.getId(), request);
						tenant2subscriptionId2IntervalSubscription.remove(tenant, request.getId());
					}
					subscriptionId2RequestGlobal.put(request.getId(), request);
				} catch (Exception e) {
					logger.error("Failed to load stored subscription " + subId);
				}
				return Uni.createFrom().voidItem();
			});
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
			return ldService.parsePure(t.getItem3().get(NGSIConstants.JSON_LD_CONTEXT)).onItem().transformToUni(ctx -> {
				SubscriptionRequest request;
				try {
					request = new SubscriptionRequest(tenant, t.getItem1(), ctx);
				} catch (ResponseException e) {
					logger.error("Failed to reload subscription " + id);
					return Uni.createFrom().voidItem();
				}
				request.setSendTimestamp(-1);
				request.setContextId(t.getItem2());
				if (isIntervalSub(request)) {
					this.tenant2subscriptionId2IntervalSubscription.put(request.getTenant(), request.getId(), request);
				} else {
					this.tenant2subscriptionId2Subscription.put(request.getTenant(), request.getId(), request);
				}
				subscriptionId2RequestGlobal.put(request.getId(), request);
				return Uni.createFrom().voidItem();
			});
		}).subscribe().with(i -> {
			logger.debug("Reloaded subscription: " + id);
		});
	}

	protected boolean shouldFireReg(Map<String, Object> entry, SubscriptionRequest subscription) {
		Set<String> attribs = subscription.getSubscription().getAttributeNames();

		if (attribs == null || attribs.isEmpty()) {
			return true;
		}
		if (entry.containsKey(NGSIConstants.NGSI_LD_INFORMATION)) {
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
		}
		// TODO add aditional changes on what could fire in a csource reg
		return false;
	}

}
