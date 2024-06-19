package eu.neclab.ngsildbroker.historyentitymanager.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import io.vertx.core.http.impl.headers.HeadersMultiMap;
import io.vertx.mutiny.core.MultiMap;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import org.apache.commons.lang3.ArrayUtils;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.locationtech.spatial4j.shape.Shape;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLDService;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.RegistrationEntry;
import eu.neclab.ngsildbroker.commons.datatypes.RemoteHost;
import eu.neclab.ngsildbroker.commons.datatypes.requests.AppendHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BatchRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteAttrHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteAttrInstanceHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.EntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.UpdateAttrHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.results.Attrib;
import eu.neclab.ngsildbroker.commons.datatypes.results.CRUDSuccess;
import eu.neclab.ngsildbroker.commons.datatypes.results.NGSILDOperationResult;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import eu.neclab.ngsildbroker.historyentitymanager.repository.HistoryDAO;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.ext.web.client.WebClient;

@ApplicationScoped
public class HistoryEntityService {

	private final static Logger logger = LoggerFactory.getLogger(HistoryEntityService.class);

	@Inject
	HistoryDAO historyDAO;

	@ConfigProperty(name = "scorpio.directDB", defaultValue = "true")
	boolean directDB;

	@ConfigProperty(name = "scorpio.topics.temporal")
	String TEMP_TOPIC;

	@ConfigProperty(name = "scorpio.history.tokafka", defaultValue = "false")
	boolean historyToKafkaEnabled;

	@Inject
	@Channel(AppConstants.HISTORY_CHANNEL)
	MutinyEmitter<String> kafkaSenderInterface;

	@Inject
	Vertx vertx;

	@Inject
	JsonLDService ldService;

	WebClient webClient;

	private Table<String, String, List<RegistrationEntry>> tenant2CId2RegEntries = HashBasedTable.create();

	// This is needed so that @postconstruct runs on the startup thread and not on a
	// worker thread later on
	void startup(@Observes StartupEvent event) {
	}

	@PostConstruct
	void init() {
		historyDAO.getAllRegistries().onItem().transform(t -> {
			tenant2CId2RegEntries = t;
			return null;
		}).await().indefinitely();
		webClient = WebClient.create(vertx);
	}

	public Uni<NGSILDOperationResult> createEntry(String tenant, Map<String, Object> resolved,
			Context originalContext,io.vertx.core.MultiMap headersFromReq) {
		logger.debug("createMessage() :: started");
		CreateHistoryEntityRequest request = new CreateHistoryEntityRequest(tenant, resolved);
		Tuple2<Map<String, Object>, Collection<Tuple2<RemoteHost, Map<String, Object>>>> splitted = splitEntity(
				request);
		Map<String, Object> localEntity = splitted.getItem1();
		Collection<Tuple2<RemoteHost, Map<String, Object>>> remoteEntitiesAndHosts = splitted.getItem2();
		request.setPayload(localEntity);
		Uni<NGSILDOperationResult> local = historyDAO.createHistoryEntity(request).onItem().transform(updated -> {
			NGSILDOperationResult result;
			result = new NGSILDOperationResult(AppConstants.CREATE_TEMPORAL_REQUEST, request.getId());
			result.setWasUpdated(updated);
			result.addSuccess(new CRUDSuccess(null, null, null, resolved, originalContext));
			return result;
		});
		if (remoteEntitiesAndHosts.isEmpty()) {
			return local;
		}
		List<Uni<NGSILDOperationResult>> unis = new ArrayList<>(remoteEntitiesAndHosts.size());
		for (Tuple2<RemoteHost, Map<String, Object>> remoteEntityAndHost : remoteEntitiesAndHosts) {
			Map<String, Object> expanded = remoteEntityAndHost.getItem2();
			RemoteHost remoteHost = remoteEntityAndHost.getItem1();
			MultiMap toFrwd = HttpUtils.getHeadToFrwd(remoteHost.headers(),headersFromReq);

			unis.add(EntityTools.prepareSplitUpEntityForSending(expanded, originalContext, ldService).onItem()
					.transformToUni(compacted -> {
						return webClient.post(remoteHost.host() + NGSIConstants.NGSI_LD_TEMPORAL_ENTITIES_ENDPOINT)
								.putHeaders(toFrwd).sendJsonObject(new JsonObject(compacted))
								.onItemOrFailure().transform((response, failure) -> {
									NGSILDOperationResult result = HttpUtils.handleWebResponse(response, failure,
											ArrayUtils.toArray(201, 204), remoteHost,
											AppConstants.CREATE_TEMPORAL_REQUEST, request.getId(),
											HttpUtils.getAttribsFromCompactedPayload(compacted));
									if (response.statusCode() == 204) {
										result.setWasUpdated(true);
									}
									return result;
								});
					}));

		}
		return Uni.combine().all().unis(unis).combinedWith(list -> {
			NGSILDOperationResult result = new NGSILDOperationResult(AppConstants.CREATE_TEMPORAL_REQUEST,
					request.getId());
			list.forEach(obj -> {
				NGSILDOperationResult opResult = (NGSILDOperationResult) obj;
				if (opResult.isWasUpdated()) {
					result.setWasUpdated(true);
				}
				result.getSuccesses().addAll(opResult.getSuccesses());
				result.getFailures().addAll(opResult.getFailures());

			});
			return result;
		});

	}

	public Uni<NGSILDOperationResult> appendToEntry(String tenant, String entityId, Map<String, Object> appendEntry,
			Context originalContext,io.vertx.core.MultiMap headersFromReq) {
		AppendHistoryEntityRequest request = new AppendHistoryEntityRequest(tenant, appendEntry, entityId);
		Tuple2<Map<String, Object>, Collection<Tuple2<RemoteHost, Map<String, Object>>>> splitted = splitEntity(
				request);
		Map<String, Object> localEntity = splitted.getItem1();
		Collection<Tuple2<RemoteHost, Map<String, Object>>> remoteEntitiesAndHosts = splitted.getItem2();
		request.setPayload(localEntity);
		Uni<NGSILDOperationResult> local = historyDAO.appendToHistoryEntity(request).onItem().transform(v -> {
			NGSILDOperationResult result;
			result = new NGSILDOperationResult(AppConstants.APPEND_TEMPORAL_REQUEST, request.getId());
			result.addSuccess(new CRUDSuccess(null, null, null, appendEntry, originalContext));
			return result;
		});
		if (remoteEntitiesAndHosts.isEmpty()) {
			return local;
		}
		List<Uni<NGSILDOperationResult>> unis = new ArrayList<>(remoteEntitiesAndHosts.size());
		for (Tuple2<RemoteHost, Map<String, Object>> remoteEntityAndHost : remoteEntitiesAndHosts) {
			Map<String, Object> expanded = remoteEntityAndHost.getItem2();
			RemoteHost remoteHost = remoteEntityAndHost.getItem1();
			MultiMap toFrwd = HttpUtils.getHeadToFrwd(remoteHost.headers(),headersFromReq);

			unis.add(EntityTools.prepareSplitUpEntityForSending(expanded, originalContext, ldService).onItem()
					.transformToUni(compacted -> {
						return webClient
								.post(remoteHost.host() + NGSIConstants.NGSI_LD_TEMPORAL_ENTITIES_ENDPOINT + "/"
										+ NGSIConstants.QUERY_PARAMETER_ATTRS)
								.putHeaders(toFrwd).sendJsonObject(new JsonObject(compacted))
								.onItemOrFailure().transform((response, failure) -> {
									return HttpUtils.handleWebResponse(response, failure, ArrayUtils.toArray(204),
											remoteHost, AppConstants.APPEND_TEMPORAL_REQUEST, request.getId(),
											HttpUtils.getAttribsFromCompactedPayload(compacted));
								});
					}));
		}
		return Uni.combine().all().unis(unis).combinedWith(list -> {
			NGSILDOperationResult result = new NGSILDOperationResult(AppConstants.APPEND_TEMPORAL_REQUEST,
					request.getId());
			list.forEach(obj -> {
				NGSILDOperationResult opResult = (NGSILDOperationResult) obj;
				result.getSuccesses().addAll(opResult.getSuccesses());
				result.getFailures().addAll(opResult.getFailures());

			});
			return result;
		});

	}

	public Uni<NGSILDOperationResult> updateInstanceOfAttr(String tenant, String entityId, String attribId,
			String instanceId, Map<String, Object> payload, Context originalContext,io.vertx.core.MultiMap headersFromReq) {
		UpdateAttrHistoryEntityRequest request = new UpdateAttrHistoryEntityRequest(tenant, entityId, attribId,
				instanceId, payload);
		Tuple2<Map<String, Object>, Collection<Tuple2<RemoteHost, Map<String, Object>>>> splitted = splitEntity(
				request);
		Map<String, Object> localEntity = splitted.getItem1();
		Collection<Tuple2<RemoteHost, Map<String, Object>>> remoteEntitiesAndHosts = splitted.getItem2();
		request.setPayload(localEntity);
		Uni<NGSILDOperationResult> local = historyDAO.updateAttrInstanceInHistoryEntity(request).onItem()
				.transform(v -> {
					NGSILDOperationResult result;
					result = new NGSILDOperationResult(AppConstants.UPDATE_TEMPORAL_INSTANCE_REQUEST, request.getId());
					result.addSuccess(new CRUDSuccess(null, null, null, payload, originalContext));
					return result;
				});
		if (remoteEntitiesAndHosts.isEmpty()) {
			return local;
		}
		List<Uni<NGSILDOperationResult>> unis = new ArrayList<>(remoteEntitiesAndHosts.size());
		for (Tuple2<RemoteHost, Map<String, Object>> remoteEntityAndHost : remoteEntitiesAndHosts) {
			Map<String, Object> expanded = remoteEntityAndHost.getItem2();
			RemoteHost remoteHost = remoteEntityAndHost.getItem1();
			MultiMap toFrwd = HttpUtils.getHeadToFrwd(remoteHost.headers(),headersFromReq);

			unis.add(EntityTools.prepareSplitUpEntityForSending(expanded, originalContext, ldService).onItem()
					.transformToUni(compacted -> {
						return webClient
								.post(remoteHost.host() + NGSIConstants.NGSI_LD_TEMPORAL_ENTITIES_ENDPOINT + "/"
										+ NGSIConstants.QUERY_PARAMETER_ATTRS + "/" + request.getAttribName() + "/"
										+ request.getInstanceId())
								.putHeaders(toFrwd).sendJsonObject(new JsonObject(compacted))
								.onItemOrFailure().transform((response, failure) -> {
									return HttpUtils.handleWebResponse(response, failure, ArrayUtils.toArray(204),
											remoteHost, AppConstants.UPDATE_TEMPORAL_INSTANCE_REQUEST, request.getId(),
											HttpUtils.getAttribsFromCompactedPayload(compacted));
								});
					}));

		}
		return Uni.combine().all().unis(unis).combinedWith(list -> {
			NGSILDOperationResult result = new NGSILDOperationResult(AppConstants.UPDATE_TEMPORAL_INSTANCE_REQUEST,
					request.getId());
			list.forEach(obj -> {
				NGSILDOperationResult opResult = (NGSILDOperationResult) obj;
				result.getSuccesses().addAll(opResult.getSuccesses());
				result.getFailures().addAll(opResult.getFailures());

			});
			return result;
		});

	}

	public Uni<NGSILDOperationResult> deleteEntry(String tenant, String entityId, Context originalContext,io.vertx.core.MultiMap headersFromReq) {
		DeleteHistoryEntityRequest request = new DeleteHistoryEntityRequest(tenant, entityId);
		Uni<NGSILDOperationResult> local = historyDAO.deleteHistoryEntity(request).onItem().transform(v -> {
			NGSILDOperationResult result;
			result = new NGSILDOperationResult(AppConstants.DELETE_TEMPORAL_REQUEST, request.getId());
			result.addSuccess(new CRUDSuccess(null, null, null, Sets.newHashSet()));
			return result;
		});
		Set<RemoteHost> remoteHosts = getRemoteHostsForDelete(request);

		if (remoteHosts.isEmpty()) {
			return local;
		}
		List<Uni<NGSILDOperationResult>> unis = new ArrayList<>(remoteHosts.size());
		for (RemoteHost remoteHost : remoteHosts) {
			MultiMap toFrwd = HttpUtils.getHeadToFrwd(remoteHost.headers(),headersFromReq);


			unis.add(webClient
					.delete(remoteHost.host() + NGSIConstants.NGSI_LD_TEMPORAL_ENTITIES_ENDPOINT + "/"
							+ request.getId())
					.putHeaders(toFrwd).send().onItemOrFailure().transform((response, failure) -> {
						Set<Attrib> attribs = new HashSet<>();
						attribs.add(new Attrib(null, entityId));
						return HttpUtils.handleWebResponse(response, failure, ArrayUtils.toArray(204), remoteHost,
								AppConstants.DELETE_TEMPORAL_REQUEST, request.getId(), attribs);

					}));

		}
		unis.add(0, local);
		return Uni.combine().all().unis(unis).combinedWith(list -> {
			NGSILDOperationResult result = new NGSILDOperationResult(AppConstants.DELETE_TEMPORAL_REQUEST,
					request.getId());
			list.forEach(obj -> {
				NGSILDOperationResult opResult = (NGSILDOperationResult) obj;
				result.getSuccesses().addAll(opResult.getSuccesses());
				result.getFailures().addAll(opResult.getFailures());

			});
			return result;
		});
	}

	public Uni<NGSILDOperationResult> deleteAttrFromEntry(String tenant, String entityId, String attrId,
			String datasetId, boolean deleteAll, Context originalContext,io.vertx.core.MultiMap headersFromReq) {
		DeleteAttrHistoryEntityRequest request = new DeleteAttrHistoryEntityRequest(tenant, entityId, attrId, datasetId,
				deleteAll);
		Uni<NGSILDOperationResult> local = historyDAO.deleteAttrFromHistoryEntity(request).onItem().transform(v -> {
			NGSILDOperationResult result;
			result = new NGSILDOperationResult(AppConstants.DELETE_TEMPORAL_ATTRIBUTE_REQUEST, request.getId());
			result.addSuccess(new CRUDSuccess(null, null, null, Sets.newHashSet(new Attrib(attrId, datasetId))));
			return result;
		});
		Set<RemoteHost> remoteHosts = getRemoteHostsForDeleteAttrib(request);

		if (remoteHosts.isEmpty()) {
			return local;
		}
		List<Uni<NGSILDOperationResult>> unis = new ArrayList<>(remoteHosts.size());
		String compactedAttr = originalContext.compactIri(attrId);
		for (RemoteHost remoteHost : remoteHosts) {
			MultiMap toFrwd = HttpUtils.getHeadToFrwd(remoteHost.headers(),headersFromReq);

			String url = remoteHost.host() + NGSIConstants.NGSI_LD_TEMPORAL_ENTITIES_ENDPOINT + "/" + request.getId()
					+ "/" + NGSIConstants.QUERY_PARAMETER_ATTRS + "/" + compactedAttr;
			if (datasetId != null) {
				url += "?" + NGSIConstants.QUERY_PARAMETER_DATA_SET_ID + "=" + datasetId;
			} else if (deleteAll) {
				url += "?" + NGSIConstants.QUERY_PARAMETER_DELETE_ALL + "=true";
			}
			unis.add(webClient.delete(url).putHeaders(toFrwd).send().onItemOrFailure()
					.transform((response, failure) -> {
						Set<Attrib> attribs = new HashSet<>();
						attribs.add(new Attrib(null, entityId));
						return HttpUtils.handleWebResponse(response, failure, ArrayUtils.toArray(204), remoteHost,
								AppConstants.DELETE_TEMPORAL_ATTRIBUTE_REQUEST, request.getId(), attribs);

					}));
		}
		unis.add(0, local);
		return Uni.combine().all().unis(unis).combinedWith(list -> {
			NGSILDOperationResult result = new NGSILDOperationResult(AppConstants.DELETE_TEMPORAL_ATTRIBUTE_REQUEST,
					request.getId());
			list.forEach(obj -> {
				NGSILDOperationResult opResult = (NGSILDOperationResult) obj;
				result.getSuccesses().addAll(opResult.getSuccesses());
				result.getFailures().addAll(opResult.getFailures());

			});
			return result;
		});
	}

	public Uni<NGSILDOperationResult> deleteInstanceOfAttr(String tenant, String entityId, String attribId,
			String instanceId, Context originalContext,io.vertx.core.MultiMap headersFromReq) {
		DeleteAttrInstanceHistoryEntityRequest request = new DeleteAttrInstanceHistoryEntityRequest(tenant, entityId,
				attribId, instanceId);

		Uni<NGSILDOperationResult> local = historyDAO.deleteAttrInstanceInHistoryEntity(request).onItem()
				.transform(v -> {
					NGSILDOperationResult result;
					result = new NGSILDOperationResult(AppConstants.DELETE_TEMPORAL_ATTRIBUTE_INSTANCE_REQUEST,
							request.getId());
					result.addSuccess(
							new CRUDSuccess(null, null, null, Sets.newHashSet(new Attrib(attribId, instanceId))));
					return result;
				});
		Set<RemoteHost> remoteHosts = getRemoteHostsForDeleteAttribInstance(request);

		if (remoteHosts.isEmpty()) {
			return local;
		}
		List<Uni<NGSILDOperationResult>> unis = new ArrayList<>(remoteHosts.size());
		String compactedAttr = originalContext.compactIri(attribId);
		for (RemoteHost remoteHost : remoteHosts) {
			MultiMap toFrwd = HttpUtils.getHeadToFrwd(remoteHost.headers(),headersFromReq);


			String url = remoteHost.host() + NGSIConstants.NGSI_LD_TEMPORAL_ENTITIES_ENDPOINT + "/" + request.getId()
					+ "/" + NGSIConstants.QUERY_PARAMETER_ATTRS + "/" + compactedAttr + "/" + instanceId;

			unis.add(webClient.delete(url).putHeaders(toFrwd).send().onItemOrFailure()
					.transform((response, failure) -> {
						Set<Attrib> attribs = new HashSet<>();
						attribs.add(new Attrib(null, entityId));
						return HttpUtils.handleWebResponse(response, failure, ArrayUtils.toArray(204), remoteHost,
								AppConstants.DELETE_TEMPORAL_ATTRIBUTE_INSTANCE_REQUEST, request.getId(), attribs);

					}));
		}
		unis.add(0, local);
		return Uni.combine().all().unis(unis).combinedWith(list -> {
			NGSILDOperationResult result = new NGSILDOperationResult(
					AppConstants.DELETE_TEMPORAL_ATTRIBUTE_INSTANCE_REQUEST, request.getId());
			list.forEach(obj -> {
				NGSILDOperationResult opResult = (NGSILDOperationResult) obj;
				result.getSuccesses().addAll(opResult.getSuccesses());
				result.getFailures().addAll(opResult.getFailures());

			});
			return result;
		});
	}

	private Set<RemoteHost> getRemoteHostsForDelete(BaseRequest request) {
		Set<RemoteHost> result = Sets.newHashSet();
		for (List<RegistrationEntry> regEntries : tenant2CId2RegEntries.row(request.getTenant()).values()) {
			for (RegistrationEntry regEntry : regEntries) {
				if (!regEntry.deleteTemporal()) {
					continue;
				}
				if ((regEntry.eId() == null && regEntry.eIdp() == null)
						|| (regEntry.eId() != null && regEntry.eId().equals(request.getId()))
						|| (regEntry.eIdp() != null && request.getId().matches(regEntry.eIdp()))) {
					result.add(new RemoteHost(regEntry.host().host(), regEntry.host().tenant(),
							regEntry.host().headers(), regEntry.host().cSourceId(), true, false, regEntry.regMode(),
							regEntry.canDoZip(), regEntry.canDoIdQuery()));
				}
			}
		}
		return result;
	}

	private Set<RemoteHost> getRemoteHostsForDeleteAttribInstance(DeleteAttrInstanceHistoryEntityRequest request) {
		Set<RemoteHost> result = Sets.newHashSet();
		for (List<RegistrationEntry> regEntries : tenant2CId2RegEntries.row(request.getTenant()).values()) {
			for (RegistrationEntry regEntry : regEntries) {
				if (!regEntry.deleteAttrInstanceTemporal()) {
					continue;
				}
				if ((regEntry.eId() == null && regEntry.eIdp() == null)
						|| (regEntry.eId() != null && regEntry.eId().equals(request.getId()))
						|| (regEntry.eIdp() != null && request.getId().matches(regEntry.eIdp()))
								&& (regEntry.eProp() == null || regEntry.eProp().equals(request.getAttribName()))) {
					result.add(new RemoteHost(regEntry.host().host(), regEntry.host().tenant(),
							regEntry.host().headers(), regEntry.host().cSourceId(), true, false, regEntry.regMode(),
							regEntry.canDoZip(), regEntry.canDoIdQuery()));
				}
			}
		}
		return result;
	}

	private Set<RemoteHost> getRemoteHostsForDeleteAttrib(DeleteAttrHistoryEntityRequest request) {
		Set<RemoteHost> result = Sets.newHashSet();
		for (List<RegistrationEntry> regEntries : tenant2CId2RegEntries.row(request.getTenant()).values()) {
			for (RegistrationEntry regEntry : regEntries) {
				if (!regEntry.deleteAttrInstanceTemporal()) {
					continue;
				}
				if ((regEntry.eId() == null && regEntry.eIdp() == null)
						|| (regEntry.eId() != null && regEntry.eId().equals(request.getId()))
						|| (regEntry.eIdp() != null && request.getId().matches(regEntry.eIdp()))
								&& (regEntry.eProp() == null || regEntry.eProp().equals(request.getAttribName()))) {
					result.add(new RemoteHost(regEntry.host().host(), regEntry.host().tenant(),
							regEntry.host().headers(), regEntry.host().cSourceId(), true, false, regEntry.regMode(),
							regEntry.canDoZip(), regEntry.canDoIdQuery()));
				}
			}
		}
		return result;
	}

	public Uni<Void> handleInternalRequest(BaseRequest request) {
		switch (request.getRequestType()) {
			case AppConstants.CREATE_REQUEST:
				return historyDAO.createHistoryEntity(new CreateHistoryEntityRequest(request)).onItem()
						.transformToUni(b -> {
							return Uni.createFrom().voidItem();
						}).onFailure().recoverWithUni(e -> {
							logger.debug("Failed to record create", e);
							return Uni.createFrom().voidItem();
						});
			case AppConstants.APPEND_REQUEST:
			case AppConstants.UPDATE_REQUEST:
			case AppConstants.REPLACE_ENTITY_REQUEST:
			case AppConstants.REPLACE_ATTRIBUTE_REQUEST:
			case AppConstants.PARTIAL_UPDATE_REQUEST:
				return historyDAO.appendToHistoryEntity(new AppendHistoryEntityRequest(request)).onItem()
						.transformToUni(resultTable -> {
							return Uni.createFrom().voidItem();
						}).onFailure().recoverWithUni(e -> {
							logger.debug("Failed to record update", e);
							return Uni.createFrom().voidItem();
						});
			case AppConstants.DELETE_REQUEST:
				return historyDAO.setEntityDeleted(request).onFailure().recoverWithUni(e -> {
					logger.debug("Failed to record delete", e);
					return Uni.createFrom().voidItem();
				});
			case AppConstants.DELETE_ATTRIBUTE_REQUEST:
				return historyDAO.setAttributeDeleted(request).onFailure().recoverWithUni(e -> {
					logger.debug("Failed to record delete attrs", e);
					return Uni.createFrom().voidItem();
				});
			case AppConstants.MERGE_PATCH_REQUEST:
				return historyDAO.setMergePatch(request).onFailure().recoverWithUni(e -> {
					logger.debug("Failed to record merge patch", e);
					return Uni.createFrom().voidItem();
				});
			default:
				return Uni.createFrom().voidItem();
			}
	}

	public Uni<Void> handleInternalBatchRequest(BatchRequest request) {
		switch (request.getRequestType()) {
		case AppConstants.CREATE_REQUEST:
		case AppConstants.APPEND_REQUEST:
		case AppConstants.UPDATE_REQUEST:
		case AppConstants.UPSERT_REQUEST:
			return historyDAO.batchUpsertHistoryEntity(request).onFailure().recoverWithUni(e -> {
				logger.debug("Failed to record create", e);
				return Uni.createFrom().voidItem();
			});
		case AppConstants.DELETE_REQUEST:
			return historyDAO.setDeletedBatchHistoryEntity(request).onFailure().recoverWithUni(e -> {
				logger.debug("Failed to record delete", e);
				return Uni.createFrom().voidItem();
			});
		default:
			return Uni.createFrom().voidItem();
		}
	}

	@SuppressWarnings("unchecked")
	private Tuple2<Map<String, Object>, Collection<Tuple2<RemoteHost, Map<String, Object>>>> splitEntity(
			EntityRequest request) {
		Map<String, Object> originalEntity = request.getPayload();
		Collection<List<RegistrationEntry>> tenantRegs = tenant2CId2RegEntries.row(request.getTenant()).values();

		Object originalScopes = originalEntity.remove(NGSIConstants.NGSI_LD_SCOPE);
		String entityId = (String) originalEntity.remove(NGSIConstants.JSON_LD_ID);
		List<String> originalTypes = (List<String>) originalEntity.remove(NGSIConstants.JSON_LD_TYPE);
		Map<String, Tuple2<RemoteHost, Map<String, Object>>> cId2RemoteHostEntity = Maps.newHashMap();
		Shape location = null;
		Set<String> toBeRemoved = Sets.newHashSet();
		for (Entry<String, Object> entry : originalEntity.entrySet()) {
			for (List<RegistrationEntry> regs : tenantRegs) {
				Iterator<RegistrationEntry> it = regs.iterator();
				while (it.hasNext()) {
					RegistrationEntry regEntry = it.next();
					if (regEntry.expiresAt() > System.currentTimeMillis()) {
						it.remove();
						continue;
					}
					switch (request.getRequestType()) {
					case AppConstants.CREATE_TEMPORAL_REQUEST:
						if (!regEntry.upsertTemporal()) {
							continue;
						}
						break;
					case AppConstants.APPEND_TEMPORAL_REQUEST:
						if (!regEntry.appendAttrsTemporal()) {
							continue;
						}
						break;
					case AppConstants.UPDATE_TEMPORAL_INSTANCE_REQUEST:
						if (!regEntry.updateAttrsTemporal()) {
							continue;
						}
						break;
					case AppConstants.DELETE_TEMPORAL_REQUEST:
						if (!regEntry.deleteTemporal()) {
							continue;
						}
						break;
					case AppConstants.DELETE_TEMPORAL_ATTRIBUTE_REQUEST:
						if (!regEntry.deleteAttrsTemporal()) {
							continue;
						}
						break;
					case AppConstants.DELETE_TEMPORAL_ATTRIBUTE_INSTANCE_REQUEST:
						if (!regEntry.deleteAttrInstanceTemporal()) {
							continue;
						}
						break;
					default:
						continue;
					}

					String propType = ((List<String>) ((List<Map<String, Object>>) entry.getValue()).get(0)
							.get(NGSIConstants.JSON_LD_TYPE)).get(0);
					Tuple2<Set<String>, Set<String>> matches;
					if (propType.equals(NGSIConstants.NGSI_LD_RELATIONSHIP)) {
						matches = regEntry.matches(entityId, originalTypes, null, entry.getKey(), originalScopes,
								location);
					} else {
						matches = regEntry.matches(entityId, originalTypes, entry.getKey(), null, originalScopes,
								location);
					}
					if (matches != null) {
						Map<String, Object> tmp;
						if (cId2RemoteHostEntity.containsKey(regEntry.cId())) {
							tmp = cId2RemoteHostEntity.get(regEntry.cId()).getItem2();
							if (matches.getItem1() != null) {
								((Set<String>) tmp.get(NGSIConstants.JSON_LD_TYPE)).addAll(matches.getItem1());
							}
							if (matches.getItem2() != null) {
								if (!tmp.containsKey(NGSIConstants.NGSI_LD_SCOPE)) {
									tmp.put(NGSIConstants.NGSI_LD_SCOPE, matches.getItem2());
								} else {
									((Set<String>) tmp.get(NGSIConstants.NGSI_LD_SCOPE)).addAll(matches.getItem2());
								}

							}
						} else {
							RemoteHost regHost = regEntry.host();
							RemoteHost host;
							switch (request.getRequestType()) {
							case AppConstants.CREATE_TEMPORAL_REQUEST:
								host = new RemoteHost(regHost.host(), regHost.tenant(), regHost.headers(),
										regHost.cSourceId(), regEntry.upsertTemporal(), false, regEntry.regMode(),
										regEntry.canDoZip(), regEntry.canDoIdQuery());
								break;
							case AppConstants.APPEND_TEMPORAL_REQUEST:
								host = new RemoteHost(regHost.host(), regHost.tenant(), regHost.headers(),
										regHost.cSourceId(), regEntry.appendAttrsTemporal(), false, regEntry.regMode(),
										regEntry.canDoZip(), regEntry.canDoIdQuery());
								break;
							case AppConstants.UPDATE_TEMPORAL_INSTANCE_REQUEST:
								host = new RemoteHost(regHost.host(), regHost.tenant(), regHost.headers(),
										regHost.cSourceId(), regEntry.updateAttrsTemporal(), false, regEntry.regMode(),
										regEntry.canDoZip(), regEntry.canDoIdQuery());
								break;
							case AppConstants.DELETE_TEMPORAL_REQUEST:
								host = new RemoteHost(regHost.host(), regHost.tenant(), regHost.headers(),
										regHost.cSourceId(), regEntry.deleteTemporal(), false, regEntry.regMode(),
										regEntry.canDoZip(), regEntry.canDoIdQuery());
								break;
							case AppConstants.DELETE_TEMPORAL_ATTRIBUTE_REQUEST:
								host = new RemoteHost(regHost.host(), regHost.tenant(), regHost.headers(),
										regHost.cSourceId(), regEntry.deleteAttrsTemporal(), false, regEntry.regMode(),
										regEntry.canDoZip(), regEntry.canDoIdQuery());
								break;
							case AppConstants.DELETE_TEMPORAL_ATTRIBUTE_INSTANCE_REQUEST:
								host = new RemoteHost(regHost.host(), regHost.tenant(), regHost.headers(),
										regHost.cSourceId(), regEntry.deleteAttrInstanceTemporal(), false,
										regEntry.regMode(), regEntry.canDoZip(), regEntry.canDoIdQuery());
								break;
							default:
								return null;
							}

							tmp = Maps.newHashMap();
							tmp.put(NGSIConstants.JSON_LD_ID, entityId);
							if (matches.getItem1() != null) {
								tmp.put(NGSIConstants.JSON_LD_TYPE, matches.getItem1());
							}
							if (matches.getItem2() != null) {
								tmp.put(NGSIConstants.NGSI_LD_SCOPE, matches.getItem2());
							}
							cId2RemoteHostEntity.put(regEntry.cId(), Tuple2.of(host, tmp));
						}
						tmp.put(entry.getKey(), entry.getValue());
						if (regEntry.regMode() > 1) {
							toBeRemoved.add(entry.getKey());
							if (regEntry.regMode() == 3) {
								break;
							}
						}
					}
				}

			}
		}
		Iterator<String> it2 = toBeRemoved.iterator();
		while (it2.hasNext()) {
			originalEntity.remove(it2.next());
		}
		Map<String, Object> toStore = null;
		if (originalEntity.size() > 0) {
			if (cId2RemoteHostEntity.isEmpty()) {
				toStore = originalEntity;
			} else {
				toStore = MicroServiceUtils.deepCopyMap(originalEntity);
			}
			toStore.put(NGSIConstants.JSON_LD_ID, entityId);
			toStore.put(NGSIConstants.JSON_LD_TYPE, originalTypes);
			if (originalScopes != null) {
				toStore.put(NGSIConstants.NGSI_LD_SCOPE, originalScopes);
			}
			EntityTools.addSysAttrs(toStore, request.getSendTimestamp());
		}
		return Tuple2.of(toStore, cId2RemoteHostEntity.values());
	}

	public Uni<Void> handleRegistryChange(BaseRequest req) {
		tenant2CId2RegEntries.remove(req.getTenant(), req.getId());
		if (req.getRequestType() != AppConstants.DELETE_REQUEST) {
			List<RegistrationEntry> newRegs = Lists.newArrayList();
			for (RegistrationEntry regEntry : RegistrationEntry.fromRegPayload(req.getPayload())) {
				if (regEntry.appendAttrsTemporal() || regEntry.deleteAttrsTemporal()
						|| regEntry.deleteAttrInstanceTemporal() || regEntry.deleteTemporal()
						|| regEntry.updateAttrsTemporal() || regEntry.upsertTemporal()) {
					newRegs.add(regEntry);
				}
			}
			tenant2CId2RegEntries.put(req.getTenant(), req.getId(), newRegs);
		}
		return Uni.createFrom().voidItem();
	}

}
