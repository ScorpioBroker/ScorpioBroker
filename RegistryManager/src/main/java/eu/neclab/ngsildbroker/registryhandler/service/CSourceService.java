package eu.neclab.ngsildbroker.registryhandler.service;

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

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.AppendCSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateCSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteCSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.results.CRUDSuccess;
import eu.neclab.ngsildbroker.commons.datatypes.results.NGSILDOperationResult;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AttrsQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.CSFQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.GeoQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.QQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.ScopeQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TypeQueryTerm;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import eu.neclab.ngsildbroker.registryhandler.controller.RegistryController;
import eu.neclab.ngsildbroker.registryhandler.repository.CSourceDAO;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.unchecked.Unchecked;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.annotations.Broadcast;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.WebClient;

@Singleton
public class CSourceService {

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

	@Inject
	Vertx vertx;

	Map<String, Object> myRegistryInformation;
	HashMap<String, TimerTask> regId2TimerTask = new HashMap<String, TimerTask>();
	Timer watchDog = new Timer(true);
	private ArrayBlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<Runnable>(50000, true);
	ThreadPoolExecutor executor = new ThreadPoolExecutor(20, 50, 600000, TimeUnit.MILLISECONDS, workQueue);
	private Table<String, Map<String, Object>, Set<String>> tenant2InformationEntry2EntityIds = HashBasedTable.create();
	private Table<String, String, Map<String, Object>> tenant2EntityId2InformationEntry = HashBasedTable.create();

	private WebClient webClient;

	@PostConstruct
	void setup() {
		this.webClient = new WebClient(vertx);
	}

//TODO handle expiration in the DB this is stupid to do in code
//	public void csourceTimerTask(ArrayListMultimap<String, String> headers, Map<String, Object> registration) {
//		Object expiresAt = registration.get(NGSIConstants.NGSI_LD_EXPIRES);
//		String regId = (String) registration.get(NGSIConstants.JSON_LD_ID);
//		if (expiresAt != null) {
//			TimerTask cancel = new TimerTask() {
//				@Override
//				public void run() {
//					try {
//						synchronized (this) {
//							deleteEntry(headers, regId);
//						}
//					} catch (Exception e) {
//						logger.error("Timer Task -> Exception while expiring residtration :: ", e);
//					}
//				}
//			};
//			regId2TimerTask.put(regId, cancel);
//			watchDog.schedule(cancel, getMillisFromDateTime(expiresAt) - System.currentTimeMillis());
//		}
//	}

	public Uni<NGSILDOperationResult> createRegistration(String tenant, Map<String, Object> registration) {
		CreateCSourceRequest request = new CreateCSourceRequest(tenant, registration);
		return cSourceInfoDAO.createRegistration(request).onItem().transformToUni(rowset -> {
			return kafkaSenderInterface.send(request).onItem().transform(v -> {
				NGSILDOperationResult result = new NGSILDOperationResult(AppConstants.OPERATION_CREATE_REGISTRATION,
						(String) registration.get(NGSIConstants.JSON_LD_ID));
				result.addSuccess(new CRUDSuccess(null, null));
				return result;
			});
		}).onFailure().recoverWithUni(
				// TODO do some proper error handling depending on the sql code
				e -> Uni.createFrom()
						.failure(new ResponseException(ErrorType.AlreadyExists, "Registration already exists")));
	}

	public Uni<NGSILDOperationResult> updateRegistration(String tenant, String registrationId,
			Map<String, Object> entry) {
		AppendCSourceRequest request = new AppendCSourceRequest(tenant, registrationId, entry);
		return cSourceInfoDAO.updateRegistration(request).onItem().transformToUni(rowset -> {
			if (rowset.rowCount() > 0) {
				// no need to query regs again they are not distributed
				request.setPayload(rowset.iterator().next().getJsonObject(0).getMap());
				return kafkaSenderInterface.send(request).onItem().transform(v -> {
					NGSILDOperationResult result = new NGSILDOperationResult(AppConstants.OPERATION_UPDATE_REGISTRATION,
							registrationId);
					result.addSuccess(new CRUDSuccess(null, null));
					return result;
				});
			} else {
				return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound, "Registration not found"));
			}
		}).onFailure().recoverWithUni(
				// TODO do some proper error handling depending on the sql code
				e -> Uni.createFrom().failure(new ResponseException(ErrorType.NotFound, "Registration not found")));
	}

	public Uni<Map<String, Object>> retrieveRegistration(String tenant, String registrationId) {
		return cSourceInfoDAO.getRegistrationById(tenant, registrationId).onItem().transformToUni(rowSet -> {
			if (rowSet.size() == 0) {
				return Uni.createFrom()
						.failure(new ResponseException(ErrorType.NotFound, registrationId + "was not found"));
			}
			return Uni.createFrom().item(rowSet.iterator().next().getJsonObject(0).getMap());
		});

	}

	public Uni<NGSILDOperationResult> deleteRegistration(String tenant, String registrationId) {
		DeleteCSourceRequest request = new DeleteCSourceRequest(tenant, registrationId);
		return cSourceInfoDAO.deleteRegistration(request).onItem().transformToUni(rowset -> {
			if (rowset.rowCount() > 0) {
				// add the deleted entry
				request.setPayload(rowset.iterator().next().getJsonObject(0).getMap());
				return kafkaSenderInterface.send(request).onItem().transform(v -> {
					NGSILDOperationResult result = new NGSILDOperationResult(AppConstants.OPERATION_DELETE_REGISTRATION,
							registrationId);
					result.addSuccess(new CRUDSuccess(null, null));
					return result;
				});
			} else {
				return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound, "Registration not found"));
			}
		}).onFailure().recoverWithUni(
				// TODO do some proper error handling depending on the sql code
				e -> Uni.createFrom().failure(new ResponseException(ErrorType.NotFound, "Registration not found")));
	}

	public Uni<List<Map<String, Object>>> queryRegistrations(String tenant, Set<String> id, TypeQueryTerm typeQuery,
			String idPattern, AttrsQueryTerm attrsQuery, QQueryTerm qQuery, CSFQueryTerm csf, GeoQueryTerm geoQuery,
			ScopeQueryTerm scopeQuery, String lang, int limit, int offSet, boolean count, boolean localOnly,
			Context context) {
		/// well the spec is not that clear so we make this up as we see fit
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
				if (regEntry.getPayload() == null) {
					deleteRegistration(regEntry.getTenant(), regEntry.getId()).await().indefinitely();
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
		Map<String, Object> informationEntry = getInformationFromEntity(message.getPayload());
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
				if (regEntry.getPayload() == null) {
					deleteRegistration(regEntry.getTenant(), regEntry.getId()).await().indefinitely();
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
		if (tmp.isEmpty()) {
			return new DeleteCSourceRequest(tenant, id);
		}
		resolved.put(NGSIConstants.NGSI_LD_INFORMATION, tmp);

		return new CreateCSourceRequest(tenant, resolved);

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

		updateRegistration(regEntry.getTenant(), regEntry.getId(), regEntry.getPayload()).onItem().transform(t -> true)
				.onFailure().recoverWithUni(e -> {
					if (e instanceof ResponseException) {
						ResponseException e1 = (ResponseException) e;

						if (e1.getErrorCode() == HttpResponseStatus.NOT_FOUND.code()) {
							return createRegistration(regEntry.getTenant(), regEntry.getPayload()).onItem()
									.transform(i -> true);
						}
					}
					return Uni.createFrom().failure(e);
				}).onFailure().recoverWithItem(e -> {
					logger.error("Failed to store internal regentry", e);
					return false;
				}).onItem().transformToUni(Unchecked.function(t -> {
					if (t && !fedBrokers.equals(AppConstants.INTERNAL_NULL_KEY) && !fedBrokers.isBlank()) {
						List<Uni<Void>> unis = Lists.newArrayList();
						for (String fedBroker : fedBrokers.split(",")) {
							String finalFedBroker;
							if (!fedBroker.endsWith("/")) {
								finalFedBroker = fedBroker + "/";
							} else {
								finalFedBroker = fedBroker;
							}
							HashMap<String, Object> copyToSend = Maps.newHashMap(regEntry.getPayload());
							String csourceId = microServiceUtils.getGatewayURL().toString();
							copyToSend.put(NGSIConstants.JSON_LD_ID, csourceId);
							String body = JsonUtils
									.toPrettyString(JsonLdProcessor.compact(copyToSend, null, HttpUtils.opts));
							unis.add(webClient.patchAbs(finalFedBroker + "csourceRegistrations/" + csourceId)
									.putHeader("Content-Type", "application/json").sendBuffer(Buffer.buffer(body))
									.onItem().transformToUni(i -> {
										if (i.statusCode() == HttpResponseStatus.NOT_FOUND.code()) {
											return webClient.postAbs(finalFedBroker + "csourceRegistrations/")
													.putHeader("Content-Type", "application/json")
													.sendBuffer(Buffer.buffer(body)).onItem().transformToUni(r -> {
														if (r.statusCode() >= 200 && r.statusCode() < 300) {
															return Uni.createFrom().nullItem();
														}
														return Uni.createFrom().failure(new ResponseException(
																ErrorType.InternalError, r.bodyAsString()));
													});
										}
										return Uni.createFrom().voidItem();
									}).onFailure().retry().atMost(5).onFailure().recoverWithUni(e -> {
										logger.error("Failed to register with fed broker", e);
										return Uni.createFrom().voidItem();
									}));
						}
						return Uni.combine().all().unis(unis).collectFailures()
								.combinedWith(l -> Uni.createFrom().voidItem());
					}
					return Uni.createFrom().voidItem();
				})).onFailure().recoverWithUni(e -> {
					logger.error("Failed to register with fed broker", e);
					return Uni.createFrom().nullItem();
				}).await().indefinitely();

	}

}
