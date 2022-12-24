package eu.neclab.ngsildbroker.registryhandler.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.el.MethodNotFoundException;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;
import eu.neclab.ngsildbroker.commons.datatypes.requests.AppendCSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateCSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteCSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.results.CreateResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.UpdateResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.EntryCRUDService;
import eu.neclab.ngsildbroker.commons.querybase.BaseQueryService;
import eu.neclab.ngsildbroker.commons.storage.StorageDAO;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;
import eu.neclab.ngsildbroker.registryhandler.controller.RegistryController;
import eu.neclab.ngsildbroker.registryhandler.repository.CSourceDAO;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.mutiny.unchecked.Unchecked;
import io.smallrye.mutiny.vertx.UniHelper;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.annotations.Broadcast;
import io.vertx.core.buffer.Buffer;

@Singleton
public class CSourceService extends BaseQueryService implements EntryCRUDService {

	private final static Logger logger = LoggerFactory.getLogger(RegistryController.class);

	@Inject
	MicroServiceUtils microServiceUtils;

	@Inject
	CSourceDAO cSourceInfoDAO;

	@Inject
	@Channel(AppConstants.REGISTRY_CHANNEL)
	@Broadcast
	MutinyEmitter<BaseRequest> kafkaSenderInterface;

	@ConfigProperty(name = "scorpio.registry.autoregmode", defaultValue = "types")
	String AUTO_REG_MODE;

	@ConfigProperty(name = "scorpio.registry.autorecording", defaultValue = "active")
	String AUTO_REG_STATUS;

	@ConfigProperty(name = "scorpio.topics.registry")
	String CSOURCE_TOPIC;

	@ConfigProperty(name = "scorpio.directDB", defaultValue = "true")
	boolean directDB;

	@ConfigProperty(name = "scorpio.fedbrokers", defaultValue = AppConstants.INTERNAL_NULL_KEY)
	String fedBrokers;

	Map<String, Object> myRegistryInformation;
	HashMap<String, TimerTask> regId2TimerTask = new HashMap<String, TimerTask>();
	Timer watchDog = new Timer(true);
	private ArrayBlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<Runnable>(50000, true);
	ThreadPoolExecutor executor = new ThreadPoolExecutor(20, 50, 600000, TimeUnit.MILLISECONDS, workQueue);
	private Table<String, Map<String, Object>, Set<String>> tenant2InformationEntry2EntityIds = HashBasedTable.create();
	private Table<String, String, Map<String, Object>> tenant2EntityId2InformationEntry = HashBasedTable.create();

//	@SuppressWarnings("unused")
//	private void loadStoredEntitiesDetails() throws IOException, ResponseException {
//		// this.csourceIds = csourceInfoDAO.getAllIds();
//		if (AUTO_REG_STATUS.equals("active")) {
//			Map<String, List<String>> tenant2Entity = csourceInfoDAO.getAllEntities();
//			for (Entry<String, List<String>> entry : tenant2Entity.entrySet()) {
//				String tenant = entry.getKey();
//				List<String> entityList = entry.getValue();
//				if (entityList.isEmpty()) {
//					continue;
//				}
//				for (String entityString : entityList) {
//					Map<String, Object> entity = (Map<String, Object>) JsonUtils.fromString(entityString);
//					Map<String, Object> informationEntry = getInformationFromEntity(entity);
//					String id = (String) entity.get(NGSIConstants.JSON_LD_ID);
//					tenant2EntityId2InformationEntry.put(tenant, id, informationEntry);
//					Set<String> ids = tenant2InformationEntry2EntityIds.get(tenant, informationEntry);
//					if (ids == null) {
//						ids = new HashSet<String>();
//					}
//					ids.add(id);
//					tenant2InformationEntry2EntityIds.put(tenant, informationEntry, ids);
//				}
//				CSourceRequest regEntry = createInternalRegEntry(tenant);
//				try {
//					storeInternalEntry(regEntry);
//				} catch (Exception e) {
//					logger.error("Failed to create initial internal reg status", e);
//				}
//			}
//		}
//	}

	public void csourceTimerTask(String tenant, Map<String, Object> registration) {
		Object expiresAt = registration.get(NGSIConstants.NGSI_LD_EXPIRES);
		String regId = (String) registration.get(NGSIConstants.JSON_LD_ID);
		if (expiresAt != null) {
			TimerTask cancel = new TimerTask() {
				@Override
				public void run() {
					try {
						synchronized (this) {
							deleteEntry(tenant, regId);
						}
					} catch (Exception e) {
						logger.error("Timer Task -> Exception while expiring residtration :: ", e);
					}
				}
			};
			regId2TimerTask.put(regId, cancel);
			watchDog.schedule(cancel, getMillisFromDateTime(expiresAt) - System.currentTimeMillis());
		}
	}

	public Uni<UpdateResult> updateEntry(String tenant, String registrationId, Map<String, Object> entry) {
		return updateEntry(tenant, registrationId, entry, new BatchInfo(-1, -1));
	}

	@Override
	public Uni<UpdateResult> updateEntry(String tenant, String registrationId, Map<String, Object> entry,
			BatchInfo batchInfo) {
		throw new MethodNotFoundException("not supported in registry");
	}

	public Uni<UpdateResult> appendToEntry(String tenant, String registrationId, Map<String, Object> entry,
			String[] options) {
		return appendToEntry(tenant, registrationId, entry, options, new BatchInfo(-1, -1));
	}

	@Override
	public Uni<UpdateResult> appendToEntry(String tenant, String registrationId, Map<String, Object> entry,
			String[] options, BatchInfo batchInfo) {
		logger.trace("appendMessage() :: started");
		// get message channel for ENTITY_APPEND topic
		// payload validation
		if (registrationId == null) {
			return Uni.createFrom()
					.failure(new ResponseException(ErrorType.BadRequestData, "empty entity id is not allowed"));
		}

		// get entity details
		return EntryCRUDService.validateIdAndGetBody(registrationId, tenant, cSourceInfoDAO).onItem()
				.transformToUni(t -> {
					try {
						return Uni.createFrom()
								.item(new AppendCSourceRequest(tenant, registrationId, t, entry, options));
					} catch (ResponseException e) {
						return Uni.createFrom().failure(e);
					}
				}).onItem().transformToUni(t -> {
					if (t.getUpdateResult().getUpdated().isEmpty()) {
						return Uni.createFrom().nullItem();
					}
					TimerTask task = regId2TimerTask.get(registrationId);
					if (task != null) {
						task.cancel();
					}
					csourceTimerTask(tenant, t.getFinalPayload());
					return handleRequest(t).onItem().transform(t2 -> {
						logger.trace("appendMessage() :: completed");
						return t.getUpdateResult();
					});
				});
	}

	public Uni<CreateResult> createEntry(String tenant, Map<String, Object> resolved) {
		return createEntry(tenant, resolved, new BatchInfo(-1, -1));
	}

	@Override
	public Uni<CreateResult> createEntry(String tenant, Map<String, Object> resolved, BatchInfo batchInfo) {

		logger.debug("createMessage() :: started");
		CSourceRequest request;
		String id;
		Object idObj = resolved.get(NGSIConstants.JSON_LD_ID);
		if (idObj == null) {
			id = EntityTools.generateUniqueRegId(resolved);
			resolved.put(NGSIConstants.JSON_LD_ID, id);
		} else {
			id = (String) idObj;
		}
		try {
			request = new CreateCSourceRequest(resolved, tenant, id);
		} catch (ResponseException e) {
			return Uni.createFrom().failure(e);
		}
		return handleRequest(request).onItem().transform(t -> {
			logger.debug("createMessage() :: completed");
			return new CreateResult(request.getId(), true);
		});

	}

	public Uni<Boolean> deleteEntry(String tenant, String registrationId) {
		return deleteEntry(tenant, registrationId, new BatchInfo(-1, -1));
	}

	@Override
	public Uni<Boolean> deleteEntry(String tenant, String registrationId, BatchInfo batchInfo) {
		logger.trace("deleteEntity() :: started");
		if (registrationId == null) {
			Uni.createFrom().failure(new ResponseException(ErrorType.BadRequestData,
					"Invalid delete for registration. No ID provided."));
		}

		return EntryCRUDService.validateIdAndGetBody(registrationId, tenant, cSourceInfoDAO).onItem()
				.transformToUni(t -> {
					try {
						return Uni.createFrom().item(Tuple2.of(new DeleteCSourceRequest(null, tenant, registrationId),
								new DeleteCSourceRequest(t, tenant, registrationId)));
					} catch (ResponseException e) {
						return Uni.createFrom().failure(e);
					}
				}).onItem().transformToUni(t -> cSourceInfoDAO.storeRegistryEntry(t.getItem1()).onItem()
						.transformToUni(i -> kafkaSenderInterface.send(t.getItem2()).onItem().transform(k -> true)));

	}

	public Uni<Map<String, Object>> getRegistrationById(String id, String tenant) {
		return cSourceInfoDAO.getRegistrationById(id, tenant);
	}

	private long getMillisFromDateTime(Object expiresAt) {
		@SuppressWarnings("unchecked")
		String value = (String) ((List<Map<String, Object>>) expiresAt).get(0).get(NGSIConstants.JSON_LD_VALUE);
		try {
			return SerializationTools.date2Long(value);
		} catch (Exception e) {
			throw new AssertionError("In invalid date time came pass the payload checker");
		}
	}

//	public Uni<QueryResult> query(QueryParams qp) {
//		return csourceInfoDAO.query(qp);
//	}

	@Override
	protected StorageDAO getQueryDAO() {
		return cSourceInfoDAO;
	}

	@Override
	protected StorageDAO getCsourceDAO() {
		// intentional null!!!
		return null;
	}

	public void handleEntityDelete(BaseRequest message) {
		String id = message.getId();
		String tenant = message.getTenant();
		Map<String, Object> informationEntry = tenant2EntityId2InformationEntry.remove(tenant, id);
		Set<String> ids = tenant2InformationEntry2EntityIds.get(tenant, informationEntry);
		if (ids == null) {
			return;
		}
		ids.remove(id);
		if (ids.isEmpty()) {
			tenant2InformationEntry2EntityIds.remove(tenant, informationEntry);
			try {
				CSourceRequest regEntry = createInternalRegEntry(tenant);
				if (regEntry.getFinalPayload() == null) {
					deleteEntry(regEntry.getTenant(), regEntry.getId()).await().indefinitely();
				} else {
					storeInternalEntry(regEntry);
				}
			} catch (Exception e) {
				logger.error("Failed to store internal registry entry", e);
			}
		} else {
			tenant2InformationEntry2EntityIds.put(tenant, informationEntry, ids);
		}
	}

	public void handleEntityCreateOrUpdate(BaseRequest message) {
		Map<String, Object> informationEntry = getInformationFromEntity(message.getFinalPayload());
		checkInformationEntry(informationEntry, message.getId(), message.getTenant());
	}

	private void checkInformationEntry(Map<String, Object> informationEntry, String id, String tenant) {
		if (informationEntry == null) {
			return;
		}
		boolean update = true;
		Set<String> ids = tenant2InformationEntry2EntityIds.get(tenant, informationEntry);
		if (ids == null) {
			ids = new HashSet<String>();
		} else {
			update = false;
		}
		ids.add(id);
		tenant2InformationEntry2EntityIds.put(tenant, informationEntry, ids);
		tenant2EntityId2InformationEntry.put(tenant, id, informationEntry);
		if (update) {
			try {
				CSourceRequest regEntry = createInternalRegEntry(tenant);
				if (regEntry.getFinalPayload() == null) {
					deleteEntry(regEntry.getTenant(), regEntry.getId()).await().indefinitely();
				} else {
					storeInternalEntry(regEntry);
				}
			} catch (Exception e) {
				logger.error("Failed to store internal registry entry", e);
			}
		}

	}

	private CSourceRequest createInternalRegEntry(String tenant) {
		String id = AppConstants.INTERNAL_REGISTRATION_ID;

		if (!tenant.equals(AppConstants.INTERNAL_NULL_KEY)) {
			id += ":" + tenant;
		}
		Map<String, Object> resolved = new HashMap<String, Object>();
		resolved.put(NGSIConstants.JSON_LD_ID, id);
		ArrayList<Object> tmp = new ArrayList<Object>();
		tmp.add(NGSIConstants.NGSI_LD_CSOURCE_REGISTRATION);
		resolved.put(NGSIConstants.JSON_LD_TYPE, tmp);
		tmp = new ArrayList<Object>();
		HashMap<String, Object> tmp2 = new HashMap<String, Object>();
		tmp2.put(NGSIConstants.JSON_LD_VALUE, microServiceUtils.getGatewayURL().toString());
		tmp.add(tmp2);
		resolved.put(NGSIConstants.NGSI_LD_ENDPOINT, tmp);
		Set<Map<String, Object>> informationEntries = tenant2InformationEntry2EntityIds.row(tenant).keySet();

		tmp = new ArrayList<Object>();
		for (Map<String, Object> entry : informationEntries) {
			tmp.add(entry);
		}
		try {
			if (tmp.isEmpty()) {
				return new DeleteCSourceRequest(null, tenant, id);
			}
			resolved.put(NGSIConstants.NGSI_LD_INFORMATION, tmp);

			return new CreateCSourceRequest(resolved, tenant, id);
		} catch (ResponseException e) {
			logger.error("failed to create internal registry entry", e);
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> getInformationFromEntity(Map<String, Object> entity) {
		HashMap<String, Object> result = new HashMap<String, Object>();
		Map<String, Object> entities = new HashMap<String, Object>();
		ArrayList<Map<String, Object>> propertyNames = new ArrayList<Map<String, Object>>();
		ArrayList<Map<String, Object>> relationshipNames = new ArrayList<Map<String, Object>>();
		for (Entry<String, Object> entry : entity.entrySet()) {
			String key = entry.getKey();
			Object value = entry.getValue();
			if (key.equals(NGSIConstants.JSON_LD_ID)) {
				if (AUTO_REG_MODE.contains("ids")) {
					entities.put(NGSIConstants.JSON_LD_ID, value);
				}
				continue;
			}
			if (key.equals(NGSIConstants.JSON_LD_TYPE)) {
				if (AUTO_REG_MODE.contains("types")) {
					entities.put(NGSIConstants.JSON_LD_TYPE, entity.get(NGSIConstants.JSON_LD_TYPE));
				}
				continue;
			}
			if (AUTO_REG_MODE.contains("attributes")) {
				if (value instanceof List) {
					Object listValue = ((List<Object>) value).get(0);
					if (listValue instanceof Map) {
						Map<String, Object> mapValue = (Map<String, Object>) listValue;
						Object type = mapValue.get(NGSIConstants.JSON_LD_TYPE);
						if (type != null) {
							String typeString;
							if (type instanceof List) {
								typeString = ((List<String>) type).get(0);
							} else if (type instanceof String) {
								typeString = (String) type;
							} else {
								continue;
							}

							HashMap<String, Object> tmp = new HashMap<String, Object>();
							tmp.put(NGSIConstants.JSON_LD_ID, entry.getKey());
							switch (typeString) {
							case NGSIConstants.NGSI_LD_GEOPROPERTY:
							case NGSIConstants.NGSI_LD_PROPERTY:
								propertyNames.add(tmp);
								break;
							case NGSIConstants.NGSI_LD_RELATIONSHIP:
								relationshipNames.add(tmp);
								break;
							default:
								continue;
							}
						}
					}
				}
			}
		}
		if (!entities.isEmpty()) {
			ArrayList<Map<String, Object>> tmp = new ArrayList<Map<String, Object>>();
			tmp.add(entities);
			result.put(NGSIConstants.NGSI_LD_ENTITIES, tmp);
		}
		if (!propertyNames.isEmpty()) {
			result.put(NGSIConstants.NGSI_LD_PROPERTIES, propertyNames);
		}
		if (!relationshipNames.isEmpty()) {
			result.put(NGSIConstants.NGSI_LD_PROPERTIES, propertyNames);
		}
		if (!result.isEmpty()) {
			return result;
		}
		return null;
	}

	private void storeInternalEntry(CSourceRequest regEntry) {

		appendToEntry(regEntry.getTenant(), regEntry.getId(), regEntry.getFinalPayload(), null).onItem()
				.transform(t -> true).onFailure().recoverWithUni(e -> {
					if (e instanceof ResponseException) {
						ResponseException e1 = (ResponseException) e;
						if (e1.getHttpStatus().equals(HttpResponseStatus.NOT_FOUND)) {
							return createEntry(regEntry.getTenant(), regEntry.getFinalPayload()).onItem()
									.transform(i -> true);
						}
					}
					return Uni.createFrom().failure(e);
				}).onFailure().recoverWithItem(e -> {
					logger.error("Failed to store internal regentry", e);
					return false;
				}).onItem().transformToUni(t -> {
					if (t && !fedBrokers.equals(AppConstants.INTERNAL_NULL_KEY) && !fedBrokers.isBlank()) {
						List<Uni<Object>> unis = Lists.newArrayList();
						for (String fedBroker : fedBrokers.split(",")) {
							String finalFedBroker;
							if (!fedBroker.endsWith("/")) {
								finalFedBroker = fedBroker + "/";
							} else {
								finalFedBroker = fedBroker;
							}
							HashMap<String, Object> copyToSend = Maps.newHashMap(regEntry.getFinalPayload());
							String csourceId = microServiceUtils.getGatewayURL().toString();
							copyToSend.put(NGSIConstants.JSON_LD_ID, csourceId);
							String body;
							try {
								body = JsonUtils.toPrettyString(JsonLdProcessor.compact(copyToSend, null, opts));
							} catch (Exception e) {
								return Uni.createFrom().failure(e);
							}
							unis.add(UniHelper.toUni(webClient
									.patchAbs(finalFedBroker + "csourceRegistrations/" + csourceId)
									.putHeader("Content-Type", "application/json").sendBuffer(Buffer.buffer(body)))
									.onItem().transformToUni(i -> {
										if (i.statusCode() == HttpResponseStatus.NOT_FOUND.code()) {
											return UniHelper
													.toUni(webClient.postAbs(finalFedBroker + "csourceRegistrations/")
															.putHeader("Content-Type", "application/json")
															.sendBuffer(Buffer.buffer(body)))
													.onItem().transformToUni(r -> {
														if (r.statusCode() >= 200 && r.statusCode() < 300) {
															return Uni.createFrom().nullItem();
														}
														return Uni.createFrom().failure(new ResponseException(
																ErrorType.InternalError, r.bodyAsString()));
													});
										}
										return Uni.createFrom().nullItem();
									}).onFailure().retry().atMost(5).onFailure().recoverWithUni(e -> {
										logger.error("Failed to register with fed broker", e);
										return Uni.createFrom().nullItem();
									}));
						}
						return Uni.combine().all().unis(unis).collectFailures()
								.combinedWith(l -> Uni.createFrom().nullItem());

					}
					return Uni.createFrom().nullItem();
				}).onFailure().recoverWithUni(e -> {
					logger.error("Failed to register with fed broker", e);
					return Uni.createFrom().nullItem();
				}).await().indefinitely();

	}

	private Uni<Void> handleRequest(CSourceRequest request) {
		return cSourceInfoDAO.storeRegistryEntry(request).onItem().transformToUni(t -> {
			request.setSendTimestamp(System.currentTimeMillis());
			return kafkaSenderInterface.send(request);
		});
	}

	@Override
	public Uni<Void> sendFail(BatchInfo batchInfo) {
		return Uni.createFrom().voidItem();
	}

}
