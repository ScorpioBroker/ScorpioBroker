package eu.neclab.ngsildbroker.entityhandler.services;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import io.vertx.core.http.impl.headers.HeadersMultiMap;
import io.vertx.mutiny.core.MultiMap;
import org.apache.commons.lang3.ArrayUtils;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.OnOverflow;
import org.eclipse.microprofile.reactive.messaging.OnOverflow.Strategy;
import org.locationtech.spatial4j.shape.Shape;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import eu.neclab.ngsildbroker.commons.datatypes.requests.AppendEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BatchRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteAttributeRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.EntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.MergePatchRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.ReplaceAttribRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.ReplaceEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.UpdateEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.UpsertEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.results.Attrib;
import eu.neclab.ngsildbroker.commons.datatypes.results.CRUDSuccess;
import eu.neclab.ngsildbroker.commons.datatypes.results.NGSILDOperationResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.annotations.Broadcast;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.HttpResponse;
import io.vertx.mutiny.ext.web.client.WebClient;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

@ApplicationScoped
public class EntityService {

	private final static Logger logger = LoggerFactory.getLogger(EntityService.class);
	public static boolean checkEntity = false;
	@ConfigProperty(name = "scorpio.directDB", defaultValue = "true")
	boolean directDB;
	@Inject
	EntityInfoDAO entityDAO;

	@Inject
	@Channel(AppConstants.ENTITY_CHANNEL)
	@Broadcast
	@OnOverflow(value = Strategy.UNBOUNDED_BUFFER)
	MutinyEmitter<String> entityEmitter;

	@Inject
	@Channel(AppConstants.ENTITY_BATCH_CHANNEL)
	@Broadcast
	@OnOverflow(value = Strategy.UNBOUNDED_BUFFER)
	MutinyEmitter<String> batchEmitter;

	@Inject
	Vertx vertx;

	WebClient webClient;

	private Table<String, String, List<RegistrationEntry>> tenant2CId2RegEntries = HashBasedTable.create();
	private Table<String, String, List<RegistrationEntry>> tenant2CId2QueryRegEntries = HashBasedTable.create();

	@Inject
	JsonLDService jsonLdService;

	@Inject
	ObjectMapper objectMapper;

	@ConfigProperty(name = "scorpio.messaging.maxSize")
	int messageSize;

	@PostConstruct
	void init() {
		webClient = WebClient.create(vertx);
		entityDAO.getAllRegistries().onItem().transform(t -> {
			tenant2CId2RegEntries = t;
			return null;
		}).await().indefinitely();
		entityDAO.getAllQueryRegistries().onItem().transform(t -> {
			tenant2CId2QueryRegEntries = t;
			return null;
		}).await().indefinitely();
	}

	// This is needed so that @postconstruct runs on the startup thread and not on a
	// worker thread later on
	void startup(@Observes StartupEvent event) {
	}

	private List<NGSILDOperationResult> handleBatchResponse(HttpResponse<Buffer> response, Throwable failure,
			RemoteHost host, List<Map<String, Object>> remoteEntities, Integer[] successCodes) {
		List<NGSILDOperationResult> result = Lists.newArrayList();
		if (failure != null) {

			for (Map<String, Object> entity : remoteEntities) {
				NGSILDOperationResult tmp = new NGSILDOperationResult(AppConstants.CREATE_REQUEST,
						entity.get("id") == null ? "no entityId" : (String) entity.get("id"));
				tmp.addFailure(new ResponseException(ErrorType.InternalError, failure.getMessage(), host,
						HttpUtils.getAttribsFromCompactedPayload(entity)));
				result.add(tmp);
			}
		} else {
			int statusCode = response.statusCode();
			if (ArrayUtils.contains(successCodes, statusCode)) {
				for (Map<String, Object> entity : remoteEntities) {
					NGSILDOperationResult tmp = new NGSILDOperationResult(AppConstants.CREATE_REQUEST,
							entity.get("id") == null ? "no entityId" : (String) entity.get("id"));
					tmp.addSuccess(new CRUDSuccess(host, HttpUtils.getAttribsFromCompactedPayload(entity)));
					result.add(tmp);
				}
			} else if (statusCode == 207) {
				JsonArray jsonArray = response.bodyAsJsonArray();
				if (jsonArray != null) {
					jsonArray.forEach(i -> {
						JsonObject jsonObj = (JsonObject) i;
						NGSILDOperationResult remoteResult;
						try {
							remoteResult = NGSILDOperationResult.getFromPayload(jsonObj.getMap());
						} catch (ResponseException e) {
							remoteResult = new NGSILDOperationResult(AppConstants.CREATE_REQUEST,
									jsonObj.getMap().get("id") == null ? "no entityId"
											: (String) jsonObj.getMap().get("id"));
							remoteResult.addFailure(e);
						}
						result.add(remoteResult);
					});
				}

			} else {
				for (Map<String, Object> entity : remoteEntities) {
					NGSILDOperationResult tmp = new NGSILDOperationResult(AppConstants.CREATE_REQUEST,
							entity.get("id") == null ? "no entityId" : (String) entity.get("id"));

					JsonObject responseBody = response.bodyAsJsonObject();

					if (responseBody == null) {
						tmp.addFailure(new ResponseException(500, NGSIConstants.ERROR_UNEXPECTED_RESULT,
								NGSIConstants.ERROR_UNEXPECTED_RESULT_NULL_TITLE, statusCode, host,
								HttpUtils.getAttribsFromCompactedPayload(entity)));

					} else {
						if (!responseBody.containsKey(NGSIConstants.ERROR_TYPE)
								|| !responseBody.containsKey(NGSIConstants.ERROR_TITLE)
								|| !responseBody.containsKey(NGSIConstants.ERROR_DETAIL)) {
							tmp.addFailure(
									new ResponseException(statusCode, responseBody.getString(NGSIConstants.ERROR_TYPE),
											responseBody.getString(NGSIConstants.ERROR_TITLE),
											responseBody.getMap().get(NGSIConstants.ERROR_DETAIL), host,
											HttpUtils.getAttribsFromCompactedPayload(entity)));
						} else {
							tmp.addFailure(new ResponseException(500, NGSIConstants.ERROR_UNEXPECTED_RESULT,
									NGSIConstants.ERROR_UNEXPECTED_RESULT_NOT_EXPECTED_BODY_TITLE,
									responseBody.getMap(), host, HttpUtils.getAttribsFromCompactedPayload(entity)));
						}
					}
					result.add(tmp);
				}
			}
		}
		return result;
	}

	private Uni<Void> handleWebResponse(NGSILDOperationResult result, HttpResponse<Buffer> response, Throwable failure,
			int successCode, RemoteHost host, Set<Attrib> attribs) {
		if (failure != null) {
			result.addFailure(new ResponseException(ErrorType.InternalError, failure.getMessage(), host, attribs));
		} else {
			int statusCode = response.statusCode();
			if (successCode == statusCode) {
				result.addSuccess(new CRUDSuccess(host, attribs));
			} else if (statusCode == 207) {
				JsonObject jsonObj = response.bodyAsJsonObject();
				if (jsonObj != null) {
					NGSILDOperationResult remoteResult;
					try {
						remoteResult = NGSILDOperationResult.getFromPayload(jsonObj.getMap());
					} catch (ResponseException e) {
						result.addFailure(e);
						return Uni.createFrom().voidItem();
					}
					result.getFailures().addAll(remoteResult.getFailures());
					result.getSuccesses().addAll(remoteResult.getSuccesses());
				}

			} else {

				JsonObject responseBody = response.bodyAsJsonObject();
				if (responseBody == null) {
					// could be from a batch response
					JsonArray tmp = response.bodyAsJsonArray();
					if (tmp != null) {
						try {
							responseBody = tmp.getJsonObject(0);
						} catch (ClassCastException e) {
							responseBody = null;
						}
					}
				}
				if (responseBody == null) {
					result.addFailure(new ResponseException(500, NGSIConstants.ERROR_UNEXPECTED_RESULT,
							NGSIConstants.ERROR_UNEXPECTED_RESULT_NULL_TITLE, statusCode, host, attribs));

				} else {
					if (!responseBody.containsKey(NGSIConstants.ERROR_TYPE)
							|| !responseBody.containsKey(NGSIConstants.ERROR_TITLE)
							|| !responseBody.containsKey(NGSIConstants.ERROR_DETAIL)) {
						result.addFailure(
								new ResponseException(statusCode, responseBody.getString(NGSIConstants.ERROR_TYPE),
										responseBody.getString(NGSIConstants.ERROR_TITLE),
										responseBody.getMap().get(NGSIConstants.ERROR_DETAIL), host, attribs));
					} else {
						result.addFailure(new ResponseException(500, NGSIConstants.ERROR_UNEXPECTED_RESULT,
								NGSIConstants.ERROR_UNEXPECTED_RESULT_NOT_EXPECTED_BODY_TITLE, responseBody.getMap(),
								host, attribs));
					}
				}
			}
		}
		return Uni.createFrom().voidItem();
	}

	public Uni<NGSILDOperationResult> partialUpdateAttribute(String tenant, String entityId, String attribName,
			Map<String, Object> payload, Context context,io.vertx.core.MultiMap headersFromReq) {
		logger.trace("updateMessage() :: started");
		Map<String, Object> effectivePayload;
		if (payload.containsKey(attribName)) {
			effectivePayload = payload;
		} else {
			effectivePayload = Maps.newHashMap();
			effectivePayload.put(attribName, Lists.newArrayList(payload));
		}
		UpdateEntityRequest request = new UpdateEntityRequest(tenant, entityId, effectivePayload, attribName);
		request.setRequestType(AppConstants.PARTIAL_UPDATE_REQUEST);
		Tuple2<Map<String, Object>, Collection<Tuple2<RemoteHost, Map<String, Object>>>> splitted = splitEntity(
				request);
		Map<String, Object> localEntity = splitted.getItem1();
		Collection<Tuple2<RemoteHost, Map<String, Object>>> remoteEntitiesAndHosts = splitted.getItem2();
//		if (remoteEntitiesAndHosts.isEmpty()) {
//			request.setPayload(localEntity);
//			return partialUpdateLocalEntity(request, context);
//		}
		List<Uni<NGSILDOperationResult>> unis = new ArrayList<>(remoteEntitiesAndHosts.size());
		for (Tuple2<RemoteHost, Map<String, Object>> remoteEntityAndHost : remoteEntitiesAndHosts) {
			Map<String, Object> expanded = remoteEntityAndHost.getItem2();

			RemoteHost remoteHost = remoteEntityAndHost.getItem1();
			MultiMap toFrwd = HttpUtils.getHeadToFrwd(remoteHost.headers(),headersFromReq);
			if (remoteHost.canDoSingleOp()) {
				unis.add(prepareSplitUpEntityForSending(expanded, context).onItem().transformToUni(compacted -> {
					return webClient
							.postAbs(remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "/" + request.getId()
									+ "/attrs/" + request.getAttribName())
							.putHeaders(toFrwd).sendJsonObject(new JsonObject(compacted))
							.onItemOrFailure().transform((response, failure) -> {
								return HttpUtils.handleWebResponse(response, failure, ArrayUtils.toArray(204),
										remoteHost, AppConstants.PARTIAL_UPDATE_REQUEST, request.getId(),
										HttpUtils.getAttribsFromCompactedPayload(compacted));
							});
				}));
			}

		}
		if (localEntity != null && !localEntity.isEmpty()) {
			if (!unis.isEmpty() && isDifferentRemoteQueryAvailable(request, remoteEntitiesAndHosts)) {
				request.setDistributed(true);
			} else {
				request.setDistributed(false);
			}
			request.setBestCompleteResult(request.getPayload());
			request.setPayload(localEntity);
			unis.add(partialUpdateLocalEntity(request, context).onFailure().recoverWithItem(e -> {
				NGSILDOperationResult localResult = new NGSILDOperationResult(AppConstants.PARTIAL_UPDATE_REQUEST,
						request.getId());
				if (e instanceof ResponseException) {
					localResult.addFailure((ResponseException) e);
				} else {
					localResult.addFailure(new ResponseException(ErrorType.InternalError, e.getMessage()));
				}

				return localResult;

			}));
		}
		return Uni.combine().all().unis(unis).combinedWith(list -> {
			return getResult(list);
		});
	}

	public Uni<Boolean> patchToEndPoint(String entityId, HttpServerRequest request, Map<String, Object> body,
			String attrId) {
		String tenantId = HttpUtils.getTenant(request);
		return entityDAO.getEndpoint(entityId, tenantId).onItem().transformToUni(endPoint -> {
			if (endPoint != null && !endPoint.equals("")) {
				WebClient webClient = WebClient.create(vertx);
				return webClient.patchAbs(endPoint + "/ngsi-ld/v1/entities/" + entityId + "/attrs/" + attrId)
						.putHeader(NGSIConstants.TENANT_HEADER, tenantId)
						.putHeader(AppConstants.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
						.sendJsonObject(new JsonObject(body)).onItem().transform(ar -> {
							logger.trace("patchToEndPoint() :: completed");
							return true;
						});
			}
			return Uni.createFrom().item(false);
		});
	}

	public Uni<NGSILDOperationResult> deleteAttribute(String tenant, String entityId, String attribName,
			String datasetId, boolean deleteAll, Context context,io.vertx.core.MultiMap headersFromReq) {
		DeleteAttributeRequest request = new DeleteAttributeRequest(tenant, entityId, attribName, datasetId, deleteAll);
		Set<RemoteHost> remoteHosts = getRemoteHostsForDeleteAttrib(request);
//		if (remoteHosts.isEmpty()) {
//			return localDeleteAttrib(request, context);
//		}
		List<Uni<NGSILDOperationResult>> unis = new ArrayList<>(remoteHosts.size());
		for (RemoteHost remoteHost : remoteHosts) {
			MultiMap toFrwd = HttpUtils.getHeadToFrwd(remoteHost.headers(),headersFromReq);
			unis.add(webClient
					.deleteAbs(remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "/" + request.getId())
					.putHeaders(toFrwd).send().onItemOrFailure().transform((response, failure) -> {
						Set<Attrib> attribs = new HashSet<>();
						attribs.add(new Attrib(null, entityId));
						return HttpUtils.handleWebResponse(response, failure, ArrayUtils.toArray(204), remoteHost,
								AppConstants.DELETE_REQUEST, request.getId(), attribs);

					}));
		}
		if (!unis.isEmpty() && isDifferentRemoteQueryAvailable(request, remoteHosts)) {
			request.setDistributed(true);
		} else {
			request.setDistributed(false);
		}
		unis.add(localDeleteAttrib(request, context));
		return Uni.combine().all().unis(unis).combinedWith(list -> getResult(list));

	}

	private boolean isDifferentRemoteQueryAvailable(BaseRequest request, Set<RemoteHost> remoteHosts) {
		if (!remoteHosts.isEmpty()) {
			return true;
		}
		return isRemoteQueryPossible(request.getPayload(), request.getTenant(), request.getId());
	}

	private Uni<NGSILDOperationResult> localDeleteAttrib(DeleteAttributeRequest request, Context context) {
		return entityDAO.deleteAttribute(request).onItem().transform(resultEntity -> {
			request.setPreviousEntity(resultEntity.getItem1());
			request.setBestCompleteResult(resultEntity.getItem2());

			MicroServiceUtils.serializeAndSplitObjectAndEmit(request, messageSize, entityEmitter, objectMapper);
			NGSILDOperationResult result = new NGSILDOperationResult(AppConstants.DELETE_ATTRIBUTE_REQUEST,
					request.getId());
			result.addSuccess(new CRUDSuccess(null, null, null,
					Set.of(new Attrib(request.getAttribName(), request.getDatasetId()))));
			return result;
		});
	}

	private Set<RemoteHost> getRemoteHostsForDeleteAttrib(DeleteAttributeRequest request) {
		Set<RemoteHost> result = Sets.newHashSet();
		for (List<RegistrationEntry> regEntries : tenant2CId2RegEntries.row(request.getTenant()).values()) {
			for (RegistrationEntry regEntry : regEntries) {
				if (!regEntry.deleteEntity() && !regEntry.deleteBatch()) {
					continue;
				}
				if (((regEntry.eId() == null && regEntry.eIdp() == null)
						|| (regEntry.eId() != null && regEntry.eId().equals(request.getId()))
						|| (regEntry.eIdp() != null && request.getId().matches(regEntry.eIdp())))
						&& ((regEntry.eProp() == null && regEntry.eRel() == null)
								|| (regEntry.eProp() != null && regEntry.eProp().equals(request.getAttribName()))
								|| (regEntry.eRel() != null && regEntry.eRel().equals(request.getAttribName()))

						)) {
					result.add(new RemoteHost(regEntry.host().host(), regEntry.host().tenant(),
							regEntry.host().headers(), regEntry.host().cSourceId(), regEntry.deleteEntity(),
							regEntry.deleteBatch(), regEntry.regMode(), regEntry.canDoZip(), regEntry.canDoIdQuery()));
				}
			}
		}
		return result;
	}

	public Uni<NGSILDOperationResult> deleteEntity(String tenant, String entityId, Context context,io.vertx.core.MultiMap headersFromReq) {
		DeleteEntityRequest request = new DeleteEntityRequest(tenant, entityId);
		Set<RemoteHost> remoteHosts = getRemoteHostsForDelete(request);

//		if (remoteHosts.isEmpty()) {
//			return localDeleteEntity(request, context);
//		}
		List<Uni<NGSILDOperationResult>> unis = new ArrayList<>(remoteHosts.size());
		for (RemoteHost remoteHost : remoteHosts) {
			MultiMap toFrwd = HttpUtils.getHeadToFrwd(remoteHost.headers(),headersFromReq);
			if (remoteHost.canDoSingleOp()) {
				unis.add(webClient
						.deleteAbs(remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "/" + request.getId())
						.putHeaders(toFrwd).send().onItemOrFailure().transform((response, failure) -> {
							Set<Attrib> attribs = new HashSet<>();
							attribs.add(new Attrib(null, entityId));
							return HttpUtils.handleWebResponse(response, failure, ArrayUtils.toArray(204), remoteHost,
									AppConstants.DELETE_REQUEST, request.getId(), attribs);

						}));
			} else {

				unis.add(webClient.postAbs(remoteHost.host() + NGSIConstants.ENDPOINT_BATCH_DELETE)
						.putHeaders(toFrwd)
						.sendJson(new JsonArray(Lists.newArrayList(new JsonObject(request.getId())))).onItemOrFailure()
						.transform((response, failure) -> {
							return handleBatchDeleteResponse(response, failure, remoteHost, List.of(request.getId()),
									ArrayUtils.toArray(204)).get(0);
						}));
			}
		}
		if (!unis.isEmpty() && isDifferentRemoteQueryAvailable(request, remoteHosts)) {
			request.setDistributed(true);
		} else {
			request.setDistributed(false);
		}

		unis.add(localDeleteEntity(request, context));
		return Uni.combine().all().unis(unis).combinedWith(list -> getResult(list));

	}

	private List<NGSILDOperationResult> handleBatchDeleteResponse(HttpResponse<Buffer> response, Throwable failure,
			RemoteHost remoteHost, List<String> of, Integer[] array) {
		// TODO Auto-generated method stub
		return null;
	}

	private Uni<NGSILDOperationResult> localDeleteEntity(DeleteEntityRequest request, Context context) {
		return entityDAO.deleteEntity(request).onItem().transform(deleted -> {
			request.setPayload(deleted);
			request.setPreviousEntity(deleted);
			MicroServiceUtils.serializeAndSplitObjectAndEmit(request, messageSize, entityEmitter, objectMapper);
			NGSILDOperationResult result = new NGSILDOperationResult(AppConstants.DELETE_REQUEST, request.getId());
			result.addSuccess(new CRUDSuccess(null, null, null, deleted, context));
			return result;
		});
	}

	private Set<RemoteHost> getRemoteHostsForDelete(DeleteEntityRequest request) {
		Set<RemoteHost> result = Sets.newHashSet();
		for (List<RegistrationEntry> regEntries : tenant2CId2RegEntries.row(request.getTenant()).values()) {
			for (RegistrationEntry regEntry : regEntries) {
				if (!regEntry.deleteEntity() && !regEntry.deleteBatch()) {
					continue;
				}
				if ((regEntry.eId() == null && regEntry.eIdp() == null)
						|| (regEntry.eId() != null && regEntry.eId().equals(request.getId()))
						|| (regEntry.eIdp() != null && request.getId().matches(regEntry.eIdp()))) {
					result.add(new RemoteHost(regEntry.host().host(), regEntry.host().tenant(),
							regEntry.host().headers(), regEntry.host().cSourceId(), regEntry.deleteEntity(),
							regEntry.deleteBatch(), regEntry.regMode(), regEntry.canDoZip(), regEntry.canDoIdQuery()));
				}
			}
		}
		return result;
	}

	public Uni<NGSILDOperationResult> appendToEntity(String tenant, String entityId, Map<String, Object> payload,
			boolean noOverwrite, Context context,io.vertx.core.MultiMap headersFromReq) {
		AppendEntityRequest request = new AppendEntityRequest(tenant, entityId, payload);
		Tuple2<Map<String, Object>, Collection<Tuple2<RemoteHost, Map<String, Object>>>> localAndRemote = splitEntity(
				request);
		Map<String, Object> localEntity = localAndRemote.getItem1();
		Collection<Tuple2<RemoteHost, Map<String, Object>>> remoteEntitiesAndHosts = localAndRemote.getItem2();
//		if (remoteEntitiesAndHosts.isEmpty()) {
//			request.setPayload(localEntity);
//			return appendLocal(request, noOverwrite, context);
//		}
		List<Uni<NGSILDOperationResult>> unis = new ArrayList<>(remoteEntitiesAndHosts.size());
		for (Tuple2<RemoteHost, Map<String, Object>> remoteEntityAndHost : remoteEntitiesAndHosts) {
			RemoteHost remoteHost = remoteEntityAndHost.getItem1();
			MultiMap toFrwd = HttpUtils.getHeadToFrwd(remoteHost.headers(),headersFromReq);
			if (remoteHost.canDoSingleOp()) {
				unis.add(prepareSplitUpEntityForSending(remoteEntityAndHost.getItem2(), context).onItem()
						.transformToUni(compacted -> {
							return webClient
									.postAbs(remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "/"
											+ request.getId() + "/attrs")
									.putHeaders(toFrwd).sendJsonObject(new JsonObject(compacted))
									.onItemOrFailure().transform((response, failure) -> {
										return HttpUtils.handleWebResponse(response, failure, ArrayUtils.toArray(204),
												remoteHost, AppConstants.APPEND_REQUEST, request.getId(),
												HttpUtils.getAttribsFromCompactedPayload(compacted));
									});
						}));
			} else {
				unis.add(prepareSplitUpEntityForSending(remoteEntityAndHost.getItem2(), context).onItem()
						.transformToUni(compacted -> {
							compacted.put(NGSIConstants.QUERY_PARAMETER_ID, request.getId());
							return webClient.postAbs(remoteHost.host() + NGSIConstants.ENDPOINT_BATCH_UPDATE)
									.putHeaders(toFrwd)
									.sendJson(new JsonArray(Lists.newArrayList(new JsonObject(compacted))))
									.onItemOrFailure().transform((response, failure) -> {
										return handleBatchResponse(response, failure, remoteHost,
												Lists.newArrayList(compacted), ArrayUtils.toArray(201)).get(0);
									});
						}));
			}

		}
		if (!localEntity.isEmpty()) {
			if (!unis.isEmpty() && isDifferentRemoteQueryAvailable(request, remoteEntitiesAndHosts)) {
				request.setDistributed(true);
			} else {
				request.setDistributed(false);
			}
			request.setBestCompleteResult(request.getPayload());
			request.setPayload(localEntity);
			unis.add(appendLocal(request, noOverwrite, context).onFailure().recoverWithItem(e -> {
				NGSILDOperationResult localResult = new NGSILDOperationResult(AppConstants.CREATE_REQUEST,
						request.getId());
				if (e instanceof ResponseException) {
					localResult.addFailure((ResponseException) e);
				} else {
					localResult.addFailure(new ResponseException(ErrorType.InternalError, e.getMessage()));
				}

				return localResult;

			}));
		}
		return Uni.combine().all().unis(unis).combinedWith(list -> {
			return getResult(list);
		});
	}

	public Uni<NGSILDOperationResult> updateEntity(String tenant, String entityId, Map<String, Object> payload,
			Context context,io.vertx.core.MultiMap headersFromReq) {
		UpdateEntityRequest request = new UpdateEntityRequest(tenant, entityId, payload, null);
		Tuple2<Map<String, Object>, Collection<Tuple2<RemoteHost, Map<String, Object>>>> localAndRemote = splitEntity(
				request);
		Map<String, Object> localEntity = localAndRemote.getItem1();
		Collection<Tuple2<RemoteHost, Map<String, Object>>> remoteEntitiesAndHosts = localAndRemote.getItem2();
//		if (remoteEntitiesAndHosts.isEmpty()) {
//			request.setPayload(localEntity);
//			return updateLocalEntity(request, context);
//		}
		List<Uni<NGSILDOperationResult>> unis = new ArrayList<>(remoteEntitiesAndHosts.size());
		for (Tuple2<RemoteHost, Map<String, Object>> remoteEntityAndHost : remoteEntitiesAndHosts) {
			Map<String, Object> expanded = remoteEntityAndHost.getItem2();
			RemoteHost remoteHost = remoteEntityAndHost.getItem1();
			MultiMap toFrwd = HttpUtils.getHeadToFrwd(remoteHost.headers(),headersFromReq);

			unis.add(prepareSplitUpEntityForSending(expanded, context).onItem().transformToUni(compacted -> {
				return webClient
						.patchAbs(remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "/" + request.getId()
								+ "/attrs")
						.putHeaders(toFrwd).sendJsonObject(new JsonObject(compacted)).onItemOrFailure()
						.transform((response, failure) -> {
							return HttpUtils.handleWebResponse(response, failure, ArrayUtils.toArray(201), remoteHost,
									AppConstants.UPDATE_REQUEST, request.getId(),
									HttpUtils.getAttribsFromCompactedPayload(compacted));
						});
			}));

		}
		if (!localEntity.isEmpty()) {
			if (!unis.isEmpty() && isDifferentRemoteQueryAvailable(request, remoteEntitiesAndHosts)) {
				request.setDistributed(true);
			} else {
				request.setDistributed(false);
			}
			request.setBestCompleteResult(request.getPayload());
			request.setPayload(localEntity);
			unis.add(updateLocalEntity(request, context).onFailure().recoverWithItem(e -> {
				NGSILDOperationResult localResult = new NGSILDOperationResult(AppConstants.UPDATE_REQUEST,
						request.getId());
				if (e instanceof ResponseException) {
					localResult.addFailure((ResponseException) e);
				} else {
					localResult.addFailure(new ResponseException(ErrorType.InternalError, e.getMessage()));
				}

				return localResult;

			}));
		}
		return Uni.combine().all().unis(unis).combinedWith(list -> {
			return getResult(list);
		});
	}

	private NGSILDOperationResult getResult(List<?> list) {
		Iterator<?> it = list.iterator();
		NGSILDOperationResult operationResult = (NGSILDOperationResult) it.next();
		while (it.hasNext()) {
			NGSILDOperationResult tmp = (NGSILDOperationResult) it.next();
			operationResult.getSuccesses().addAll(tmp.getSuccesses());
			operationResult.getFailures().addAll(tmp.getFailures());
		}
		return operationResult;
	}

	private Uni<NGSILDOperationResult> updateLocalEntity(UpdateEntityRequest request, Context context) {
		return entityDAO.updateEntity(request).onItem().transform(previousAndNewEntity -> {
			request.setPreviousEntity(previousAndNewEntity.getItem1());
			request.setBestCompleteResult(previousAndNewEntity.getItem2());
			MicroServiceUtils.serializeAndSplitObjectAndEmit(request, messageSize, entityEmitter, objectMapper);
			NGSILDOperationResult localResult = new NGSILDOperationResult(AppConstants.UPDATE_REQUEST, request.getId());
			localResult.addSuccess(new CRUDSuccess(null, null, null, request.getPayload(), context));
			return localResult;
		});
	}

	public Uni<NGSILDOperationResult> createEntity(String tenant, Map<String, Object> resolved, Context context,io.vertx.core.MultiMap headersFromReq) {
		logger.debug("createMessage() :: started");
		CreateEntityRequest request = new CreateEntityRequest(tenant, resolved);
		Tuple2<Map<String, Object>, Collection<Tuple2<RemoteHost, Map<String, Object>>>> localAndRemote = splitEntity(
				request);
		Map<String, Object> localEntity = localAndRemote.getItem1();
		Collection<Tuple2<RemoteHost, Map<String, Object>>> remoteEntitiesAndHosts = localAndRemote.getItem2();
//		if (remoteEntitiesAndHosts.isEmpty()) {
//			request.setPayload(localEntity);
//			return createLocalEntity(request, context);
//		}
		List<Uni<NGSILDOperationResult>> unis = new ArrayList<>(remoteEntitiesAndHosts.size());
		for (Tuple2<RemoteHost, Map<String, Object>> remoteEntityAndHost : remoteEntitiesAndHosts) {
			Map<String, Object> expanded = remoteEntityAndHost.getItem2();
			RemoteHost remoteHost = remoteEntityAndHost.getItem1();
			MultiMap toFrwd = HttpUtils.getHeadToFrwd(remoteHost.headers(),headersFromReq);

			if (remoteHost.canDoSingleOp()) {
				unis.add(prepareSplitUpEntityForSending(expanded, context).onItem().transformToUni(compacted -> {
					return webClient.postAbs(remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT)
							.putHeaders(toFrwd).sendJsonObject(new JsonObject(compacted))
							.onItemOrFailure().transform((response, failure) -> {
								return HttpUtils.handleWebResponse(response, failure, ArrayUtils.toArray(201),
										remoteHost, AppConstants.CREATE_REQUEST, request.getId(),
										HttpUtils.getAttribsFromCompactedPayload(compacted));
							});
				}));
			} else {
				unis.add(prepareSplitUpEntityForSending(expanded, context).onItem().transformToUni(compacted -> {
					return webClient.postAbs(remoteHost.host() + NGSIConstants.ENDPOINT_BATCH_CREATE)
							.putHeaders(toFrwd)
							.sendJson(new JsonArray(Lists.newArrayList(new JsonObject(compacted)))).onItemOrFailure()
							.transform((response, failure) -> {
								return handleBatchResponse(response, failure, remoteHost, Lists.newArrayList(compacted),
										ArrayUtils.toArray(201)).get(0);
							});
				}));
			}
		}
		if (localEntity != null && !localEntity.isEmpty()) {
			if (!unis.isEmpty() && isDifferentRemoteQueryAvailable(request, remoteEntitiesAndHosts)) {
				request.setDistributed(true);
			} else {
				request.setDistributed(false);
			}
			request.setBestCompleteResult(request.getPayload());
			request.setPayload(localEntity);
			unis.add(0, createLocalEntity(request, context).onFailure().recoverWithItem(e -> {
				NGSILDOperationResult localResult = new NGSILDOperationResult(AppConstants.CREATE_REQUEST,
						request.getId());
				if (e instanceof ResponseException) {
					localResult.addFailure((ResponseException) e);
				} else {
					localResult.addFailure(new ResponseException(ErrorType.InternalError, e.getMessage()));
				}

				return localResult;

			}));
		}
		return Uni.combine().all().unis(unis).combinedWith(list -> {
			return getResult(list);
		});
	}

	private boolean isDifferentRemoteQueryAvailable(BaseRequest request,
			Collection<Tuple2<RemoteHost, Map<String, Object>>> remoteEntitiesAndHosts) {
		if (!remoteEntitiesAndHosts.isEmpty()) {
			return true;
		}
		return isRemoteQueryPossible(request.getPayload(), request.getTenant(), request.getId());
	}

	private Uni<NGSILDOperationResult> createLocalEntity(CreateEntityRequest request, Context context) {
		return entityDAO.createEntity(request).onItem().transform(v -> {
			MicroServiceUtils.serializeAndSplitObjectAndEmit(request, messageSize, entityEmitter, objectMapper);
			NGSILDOperationResult localResult = new NGSILDOperationResult(AppConstants.CREATE_REQUEST, request.getId());
			localResult.addSuccess(new CRUDSuccess(null, null, null, request.getPayload(), context));
			return localResult;
		});
	}

	private Uni<NGSILDOperationResult> partialUpdateLocalEntity(UpdateEntityRequest request, Context context) {
		return entityDAO.partialUpdateAttribute(request).onItem().transform(v -> {
			NGSILDOperationResult localResult = new NGSILDOperationResult(AppConstants.PARTIAL_UPDATE_REQUEST,
					request.getId());
			request.setPreviousEntity(v.getItem1());
			request.setBestCompleteResult(v.getItem2());
			MicroServiceUtils.serializeAndSplitObjectAndEmit(request, messageSize, entityEmitter, objectMapper);
			localResult.addSuccess(new CRUDSuccess(null, null, null, request.getPayload(), context));
			return localResult;
		});
	}

	private Tuple2<Map<String, Object>, Collection<Tuple2<RemoteHost, Map<String, Object>>>> splitEntity(
			EntityRequest request) {
		Map<String, Object> originalEntity = request.getPayload();
		Collection<List<RegistrationEntry>> tenantRegs = tenant2CId2RegEntries.row(request.getTenant()).values();

		Object originalScopes = originalEntity.remove(NGSIConstants.NGSI_LD_SCOPE);
		String entityId = request.getId();
		originalEntity.remove(NGSIConstants.JSON_LD_ID);
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
					case AppConstants.CREATE_REQUEST:
						if (!regEntry.createEntity() && !regEntry.createBatch()) {
							continue;
						}
						break;
					case AppConstants.UPDATE_REQUEST:
					case AppConstants.MERGE_PATCH_REQUEST:
					case AppConstants.REPLACE_ENTITY_REQUEST:
						if (!regEntry.updateEntity()) {
							continue;
						}
						break;
					case AppConstants.PARTIAL_UPDATE_REQUEST:
					case AppConstants.REPLACE_ATTRIBUTE_REQUEST:
						if (!regEntry.updateAttrs()) {
							continue;
						}
						break;
					case AppConstants.APPEND_REQUEST:
						if (!regEntry.appendAttrs() && !regEntry.updateBatch()) {
							continue;
						}
						break;
					case AppConstants.UPSERT_REQUEST:
						if (!regEntry.upsertBatch() && !regEntry.appendAttrs() && !regEntry.createEntity()) {
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
							if (matches.getItem2() != null && !matches.getItem2().isEmpty()) {
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
							case AppConstants.CREATE_REQUEST:
								host = new RemoteHost(regHost.host(), regHost.tenant(), regHost.headers(),
										regHost.cSourceId(), regEntry.createEntity(), regEntry.createBatch(),
										regEntry.regMode(), regEntry.canDoZip(), regEntry.canDoIdQuery());
								break;
							case AppConstants.UPDATE_REQUEST:
								host = new RemoteHost(regHost.host(), regHost.tenant(), regHost.headers(),
										regHost.cSourceId(), regEntry.updateAttrs(), regEntry.updateBatch(),
										regEntry.regMode(), regEntry.canDoZip(), regEntry.canDoIdQuery());
								break;
							case AppConstants.MERGE_PATCH_REQUEST:
								host = new RemoteHost(regHost.host(), regHost.tenant(), regHost.headers(),
										regHost.cSourceId(), regEntry.mergeEntity(), regEntry.mergeBatch(),
										regEntry.regMode(), regEntry.canDoZip(), regEntry.canDoIdQuery());
								break;
							case AppConstants.REPLACE_ENTITY_REQUEST:
								host = new RemoteHost(regHost.host(), regHost.tenant(), regHost.headers(),
										regHost.cSourceId(), regEntry.replaceEntity(), false, regEntry.regMode(),
										regEntry.canDoZip(), regEntry.canDoIdQuery());
								break;
							case AppConstants.REPLACE_ATTRIBUTE_REQUEST:
								host = new RemoteHost(regHost.host(), regHost.tenant(), regHost.headers(),
										regHost.cSourceId(), regEntry.replaceAttrs(), false, regEntry.regMode(),
										regEntry.canDoZip(), regEntry.canDoIdQuery());
							case AppConstants.PARTIAL_UPDATE_REQUEST:
								host = new RemoteHost(regHost.host(), regHost.tenant(), regHost.headers(),
										regHost.cSourceId(), regEntry.updateAttrs(), false, regEntry.regMode(),
										regEntry.canDoZip(), regEntry.canDoIdQuery());
								break;
							case AppConstants.APPEND_REQUEST:
								host = new RemoteHost(regHost.host(), regHost.tenant(), regHost.headers(),
										regHost.cSourceId(), regEntry.appendAttrs(), regEntry.updateBatch(),
										regEntry.regMode(), regEntry.canDoZip(), regEntry.canDoIdQuery());
								break;
							case AppConstants.UPSERT_REQUEST:
								host = new RemoteHost(regHost.host(), regHost.tenant(), regHost.headers(),
										regHost.cSourceId(), (regEntry.appendAttrs() && regEntry.createEntity()),
										regEntry.upsertBatch(), regEntry.regMode(), regEntry.canDoZip(),
										regEntry.canDoIdQuery());
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
		if(originalEntity.isEmpty()){
			Map<String, Object> toStore = new HashMap<>();
			toStore.put(NGSIConstants.JSON_LD_ID, entityId);
			toStore.put(NGSIConstants.JSON_LD_TYPE, originalTypes);
			if (originalScopes != null) {
				toStore.put(NGSIConstants.NGSI_LD_SCOPE, originalScopes);
			}
			EntityTools.addSysAttrs(toStore, request.getSendTimestamp());
			return Tuple2.of(toStore, cId2RemoteHostEntity.values());
		}
        for (String s : toBeRemoved) {
            originalEntity.remove(s);
        }
		Map<String, Object> toStore = null;
		if (!originalEntity.isEmpty()) {
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
		tenant2CId2QueryRegEntries.remove(req.getTenant(), req.getId());
		if (req.getRequestType() != AppConstants.DELETE_REQUEST) {
			List<RegistrationEntry> newRegs = Lists.newArrayList();
			List<RegistrationEntry> newQueryRegs = Lists.newArrayList();
			for (RegistrationEntry regEntry : RegistrationEntry.fromRegPayload(req.getPayload())) {
				if (regEntry.createEntity() || regEntry.appendAttrs() || regEntry.createBatch()
						|| regEntry.deleteAttrs() || regEntry.deleteBatch() || regEntry.deleteEntity()
						|| regEntry.mergeBatch() || regEntry.mergeEntity() || regEntry.replaceAttrs()
						|| regEntry.replaceEntity() || regEntry.updateAttrs() || regEntry.updateBatch()
						|| regEntry.updateEntity() || regEntry.upsertBatch()) {
					newRegs.add(regEntry);
				}
				if (regEntry.queryBatch() || regEntry.queryEntity() || regEntry.retrieveEntity()) {
					newQueryRegs.add(regEntry);
				}
			}
			tenant2CId2RegEntries.put(req.getTenant(), req.getId(), newRegs);
			tenant2CId2QueryRegEntries.put(req.getTenant(), req.getId(), newQueryRegs);
		}
		return Uni.createFrom().voidItem();
	}

	private Uni<NGSILDOperationResult> appendLocal(AppendEntityRequest request, boolean noOverwrite, Context context) {
		return entityDAO.appendToEntity2(request, noOverwrite).onItem().transform(resultAndNotAppended -> {
			NGSILDOperationResult localResult = new NGSILDOperationResult(AppConstants.APPEND_REQUEST, request.getId());
			Set<Attrib> failedToAdd = Sets.newHashSet();
			Set<String> notAppended = resultAndNotAppended.getItem2();
			request.setBestCompleteResult(resultAndNotAppended.getItem1());
			Map<String, Object> payload = request.getPayload();
			for (String entry : notAppended) {
				payload.remove(entry);
				failedToAdd.add(new Attrib(context.compactIri(entry), null));
			}
			localResult.addSuccess(new CRUDSuccess(null, null, null, request.getPayload(), context));
			if (!failedToAdd.isEmpty())
				localResult.addFailure(new ResponseException(ErrorType.None, "Not added", failedToAdd));
			MicroServiceUtils.serializeAndSplitObjectAndEmit(request, messageSize, entityEmitter, objectMapper);
			return localResult;

		});
	}

	private Uni<Map<String, Object>> prepareSplitUpEntityForSending(Map<String, Object> expanded, Context context) {
		if (expanded.containsKey(NGSIConstants.JSON_LD_TYPE)) {
			expanded.put(NGSIConstants.JSON_LD_TYPE,
					Lists.newArrayList((Set<String>) expanded.get(NGSIConstants.JSON_LD_TYPE)));
		}
		if (expanded.containsKey(NGSIConstants.NGSI_LD_SCOPE)) {
			Set<String> collectedScopes = (Set<String>) expanded.get(NGSIConstants.NGSI_LD_SCOPE);
			List<Map<String, String>> finalScopes = Lists.newArrayList();
			for (String scope : collectedScopes) {
				finalScopes.add(Map.of(NGSIConstants.JSON_LD_VALUE, scope));
			}
			expanded.put(NGSIConstants.NGSI_LD_SCOPE, finalScopes);
		}
		return jsonLdService.compact(expanded, null, context, HttpUtils.opts, -1);

	}

	public Uni<List<NGSILDOperationResult>> createBatch(String tenant, List<Map<String, Object>> expandedEntities,
			List<Context> contexts, boolean localOnly,io.vertx.core.MultiMap headersFromReq) {
		Iterator<Map<String, Object>> itEntities = expandedEntities.iterator();
		Iterator<Context> itContext = contexts.iterator();
		Map<RemoteHost, List<Tuple2<Context, Map<String, Object>>>> remoteHost2Batch = Maps.newHashMap();
		List<Map<String, Object>> localEntities = Lists.newArrayList();
		while (itEntities.hasNext() && itContext.hasNext()) {
			Tuple2<Map<String, Object>, Collection<Tuple2<RemoteHost, Map<String, Object>>>> split = splitEntity(
					new CreateEntityRequest(tenant, itEntities.next()));
			Map<String, Object> local = split.getItem1();
			Context context = itContext.next();
			if (local != null) {
				localEntities.add(local);
			} else {
				itContext.remove();
			}
			Collection<Tuple2<RemoteHost, Map<String, Object>>> remotes = split.getItem2();
			for (Tuple2<RemoteHost, Map<String, Object>> remote : remotes) {
				List<Tuple2<Context, Map<String, Object>>> entities2Context;
				if (remoteHost2Batch.containsKey(remote.getItem1())) {
					entities2Context = remoteHost2Batch.get(remote.getItem1());
				} else {
					entities2Context = Lists.newArrayList();
					remoteHost2Batch.put(remote.getItem1(), entities2Context);
				}
				entities2Context.add(Tuple2.of(context, remote.getItem2()));
			}
		}

		List<Uni<List<NGSILDOperationResult>>> unis = new ArrayList<>();
		if (!localOnly) {
			for (Entry<RemoteHost, List<Tuple2<Context, Map<String, Object>>>> entry : remoteHost2Batch.entrySet()) {
				RemoteHost remoteHost = entry.getKey();
				MultiMap toFrwd = HttpUtils.getHeadToFrwd(remoteHost.headers(),headersFromReq);
				List<Tuple2<Context, Map<String, Object>>> tuples = entry.getValue();
				List<Uni<Map<String, Object>>> compactedUnis = Lists.newArrayList();
				for (Tuple2<Context, Map<String, Object>> tuple : tuples) {
					Map<String, Object> expanded = tuple.getItem2();
					Context context = tuple.getItem1();
					compactedUnis.add(jsonLdService.compact(expanded, null, context, AppConstants.opts, -1));
				}

				if (remoteHost.canDoBatchOp()) {
					unis.add(Uni.combine().all().unis(compactedUnis).combinedWith(list -> {
						List<Map<String, Object>> toSend = Lists.newArrayList();
						for (Object obj : list) {
							toSend.add((Map<String, Object>) obj);
						}
						return toSend;
					}).onItem().transformToUni(toSend -> {
						return webClient.postAbs(remoteHost.host() + NGSIConstants.ENDPOINT_BATCH_CREATE)
								.putHeaders(toFrwd).sendJson(new JsonArray(toSend)).onItemOrFailure()
								.transform((response, failure) -> {
									return handleBatchResponse(response, failure, remoteHost, toSend,
											ArrayUtils.toArray(201));
								});
					}));
				} else {
					List<Uni<NGSILDOperationResult>> singleUnis = new ArrayList<>();
					for (Uni<Map<String, Object>> compactedUni : compactedUnis) {
						singleUnis.add(compactedUni.onItem().transformToUni(entity -> {
							return webClient.postAbs(remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT)
									.putHeaders(toFrwd).sendJsonObject(new JsonObject(entity))
									.onItemOrFailure().transform((response, failure) -> {
										return HttpUtils.handleWebResponse(response, failure, ArrayUtils.toArray(201),
												remoteHost, AppConstants.CREATE_REQUEST,
												(String) entity.get(NGSIConstants.JSON_LD_ID),
												HttpUtils.getAttribsFromCompactedPayload(entity));
									});
						}));
					}
					unis.add(Uni.combine().all().unis(singleUnis).combinedWith(list -> {
						List<NGSILDOperationResult> result = Lists.newArrayList();
						list.forEach(obj -> result.add((NGSILDOperationResult) obj));
						return result;
					}));
				}
			}

		}
		BatchRequest request = new BatchRequest(tenant, localEntities, contexts, AppConstants.CREATE_REQUEST);
		if (!localEntities.isEmpty()) {
			if (!unis.isEmpty() && isDifferentRemoteQueryAvailable(request, remoteHost2Batch)) {
				request.setDistributed(true);
			} else {
				request.setDistributed(false);
			}
			unis.add(0, entityDAO.batchCreateEntity(request).onItem().transform(dbResult -> {
				List<NGSILDOperationResult> result = Lists.newArrayList();
				List<String> successes = (List<String>) dbResult.get("success");
				List<Map<String, String>> fails = (List<Map<String, String>>) dbResult.get("failure");

				for (String entityId : successes) {
					NGSILDOperationResult opResult = new NGSILDOperationResult(AppConstants.CREATE_REQUEST, entityId);
					opResult.addSuccess(new CRUDSuccess(null, null, null, Sets.newHashSet()));
					result.add(opResult);
				}
				for (Map<String, String> fail : fails) {
					fail.entrySet().forEach(entry -> {
						String entityId = entry.getKey();
						String sqlstate = entry.getValue();
						request.removeFromPayloadAndContext(entityId);
						NGSILDOperationResult opResult = new NGSILDOperationResult(AppConstants.CREATE_REQUEST,
								entityId);
						if (sqlstate.equals(AppConstants.SQL_ALREADY_EXISTS)) {
							opResult.addFailure(new ResponseException(ErrorType.AlreadyExists, entityId));
						} else {
							opResult.addFailure(new ResponseException(ErrorType.InvalidRequest, sqlstate));
						}
						result.add(opResult);
					});

				}

				if (!request.getRequestPayload().isEmpty()) {
					logger.debug("Create batch request sending to kafka " + request.getEntityIds());
					MicroServiceUtils.serializeAndSplitObjectAndEmit(request, messageSize, batchEmitter, objectMapper);
				}
				return result;
			}));
		}

		return Uni.combine().all().unis(unis).combinedWith(resultLists -> {
			List<NGSILDOperationResult> result = Lists.newArrayList();
			resultLists.forEach(resultList -> {
				result.addAll((List<NGSILDOperationResult>) resultList);
			});
			return result;
		});
	}

	public Uni<List<NGSILDOperationResult>> appendBatch(String tenant, List<Map<String, Object>> expandedEntities,
			List<Context> contexts, boolean localOnly,boolean noOverWrite,io.vertx.core.MultiMap headersFromReq) {
		Iterator<Map<String, Object>> itEntities = expandedEntities.iterator();
		Iterator<Context> itContext = contexts.iterator();
		Map<RemoteHost, List<Tuple2<Context, Map<String, Object>>>> remoteHost2Batch = Maps.newHashMap();
		List<Map<String, Object>> localEntities = Lists.newArrayList();
		while (itEntities.hasNext() && itContext.hasNext()) {
			Map<String, Object> entity = itEntities.next();
			Tuple2<Map<String, Object>, Collection<Tuple2<RemoteHost, Map<String, Object>>>> split = splitEntity(
					new AppendEntityRequest(tenant, (String) entity.get(NGSIConstants.JSON_LD_ID), entity));
			Map<String, Object> local = split.getItem1();
			Context context = itContext.next();
			if (local != null) {
				local.remove(NGSIConstants.NGSI_LD_CREATED_AT);
				localEntities.add(local);
			} else {
				itContext.remove();
			}
			Collection<Tuple2<RemoteHost, Map<String, Object>>> remotes = split.getItem2();
			for (Tuple2<RemoteHost, Map<String, Object>> remote : remotes) {
				List<Tuple2<Context, Map<String, Object>>> entities2Context;
				if (remoteHost2Batch.containsKey(remote.getItem1())) {
					entities2Context = remoteHost2Batch.get(remote.getItem1());
				} else {
					entities2Context = Lists.newArrayList();
					remoteHost2Batch.put(remote.getItem1(), entities2Context);
				}
				entities2Context.add(Tuple2.of(context, remote.getItem2()));
			}
		}

		List<Uni<List<NGSILDOperationResult>>> unis = new ArrayList<>();
		if (!localOnly) {
			for (Entry<RemoteHost, List<Tuple2<Context, Map<String, Object>>>> entry : remoteHost2Batch.entrySet()) {
				RemoteHost remoteHost = entry.getKey();
				MultiMap toFrwd = HttpUtils.getHeadToFrwd(remoteHost.headers(),headersFromReq);

				List<Tuple2<Context, Map<String, Object>>> tuples = entry.getValue();
				List<Uni<Map<String, Object>>> compactedUnis = Lists.newArrayList();
				for (Tuple2<Context, Map<String, Object>> tuple : tuples) {
					Map<String, Object> expanded = tuple.getItem2();
					Context context = tuple.getItem1();
					compactedUnis.add(jsonLdService.compact(expanded, null, context, AppConstants.opts, -1));
				}
				if (remoteHost.canDoBatchOp()) {
					unis.add(Uni.combine().all().unis(compactedUnis).combinedWith(list -> {
						List<Map<String, Object>> toSend = Lists.newArrayList();
						for (Object obj : list) {
							toSend.add((Map<String, Object>) obj);
						}
						return toSend;
					}).onItem().transformToUni(toSend -> {
						return webClient.postAbs(remoteHost.host() + NGSIConstants.ENDPOINT_BATCH_UPDATE)
								.putHeaders(toFrwd).sendJson(new JsonArray(toSend)).onItemOrFailure()
								.transform((response, failure) -> {
									return handleBatchResponse(response, failure, remoteHost, toSend,
											ArrayUtils.toArray(204));
								});
					}));
				} else {
					List<Uni<NGSILDOperationResult>> singleUnis = new ArrayList<>();
					for (Uni<Map<String, Object>> compactedUni : compactedUnis) {
						singleUnis.add(compactedUni.onItem().transformToUni(entity -> {
							return webClient
									.postAbs(remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "/"
											+ entity.get(NGSIConstants.JSON_LD_ID) + "/"
											+ NGSIConstants.QUERY_PARAMETER_ATTRS)
									.putHeaders(toFrwd).sendJsonObject(new JsonObject(entity))
									.onItemOrFailure().transform((response, failure) -> {
										return HttpUtils.handleWebResponse(response, failure, ArrayUtils.toArray(201),
												remoteHost, AppConstants.APPEND_REQUEST,
												(String) entity.get(NGSIConstants.JSON_LD_ID),
												HttpUtils.getAttribsFromCompactedPayload(entity));
									});
						}));
					}
					unis.add(Uni.combine().all().unis(singleUnis).combinedWith(list -> {
						List<NGSILDOperationResult> result = Lists.newArrayList();
						list.forEach(obj -> result.add((NGSILDOperationResult) obj));
						return result;
					}));
				}
			}
		}
		if (!localEntities.isEmpty()) {
			BatchRequest request = new BatchRequest(tenant, localEntities, contexts, AppConstants.APPEND_REQUEST);
			request.setNoOverwrite(noOverWrite);
			if (!unis.isEmpty() && isDifferentRemoteQueryAvailable(request, remoteHost2Batch)) {
				request.setDistributed(true);
			} else {
				request.setDistributed(false);
			}
			Uni<List<NGSILDOperationResult>> local = entityDAO.batchAppendEntity(request).onItem()
					.transform(dbResult -> {
						List<NGSILDOperationResult> result = Lists.newArrayList();
						List<Map<String, Object>> successes = (List<Map<String, Object>>) dbResult.get("success");
						List<Map<String, String>> fails = (List<Map<String, String>>) dbResult.get("failure");
						List<Map<String, Object>> newEntities = Lists.newArrayList();
						List<Map<String, Object>> oldEntities = Lists.newArrayList();
						for (Map<String, Object> success : successes) {
							String entityId = (String) success.get("id");
							NGSILDOperationResult opResult = new NGSILDOperationResult(AppConstants.APPEND_REQUEST,
									entityId);
							opResult.addSuccess(new CRUDSuccess(null, null, null, Sets.newHashSet()));
							result.add(opResult);
							Map<String, Object> old = (Map<String, Object>) success.get("old");
							Map<String, Object> newEntity = (Map<String, Object>) success.get("new");
							newEntities.add(newEntity);
							oldEntities.add(old);
						}
						for (Map<String, String> fail : fails) {
							fail.entrySet().forEach(entry -> {
								String entityId = entry.getKey();
								String sqlstate = entry.getValue();
								request.removeFromPayloadAndContext(entityId);
								NGSILDOperationResult opResult = new NGSILDOperationResult(AppConstants.APPEND_REQUEST,
										entityId);
								if (sqlstate.equals(AppConstants.SQL_NOT_FOUND)) {
									opResult.addFailure(new ResponseException(ErrorType.NotFound, entityId));
								} else {
									opResult.addFailure(new ResponseException(ErrorType.InvalidRequest, sqlstate));
								}
								result.add(opResult);
							});

						}
						request.setBestCompleteResult(newEntities);
						request.setPreviousEntity(oldEntities);
						if (!request.getRequestPayload().isEmpty()) {
							logger.debug("Append batch request sending to kafka " + request.getEntityIds());
							MicroServiceUtils.serializeAndSplitObjectAndEmit(request, messageSize, batchEmitter,
									objectMapper);
						}
						return result;
					});

			unis.add(0, local);
		}
		if (unis.isEmpty()) {
			return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound));
		}
		return Uni.combine().all().unis(unis).combinedWith(resultLists -> {
			List<NGSILDOperationResult> result = Lists.newArrayList();
			resultLists.forEach(resultList -> {
				result.addAll((List<NGSILDOperationResult>) resultList);
			});
			return result;
		});
	}

	private boolean isDifferentRemoteQueryAvailable(BatchRequest request,
			Map<RemoteHost, List<Tuple2<Context, Map<String, Object>>>> remoteHost2Batch) {
		if (!remoteHost2Batch.isEmpty()) {
			return true;
		}
		for (Map<String, Object> entry : (List<Map<String, Object>>) request.getBestCompleteResult()) {
			if (isRemoteQueryPossible(entry, request.getTenant(), (String) entry.get(NGSIConstants.JSON_LD_ID))) {
				return true;
			}
		}
		return false;
	}

	public Uni<List<NGSILDOperationResult>> upsertBatch(String tenant, List<Map<String, Object>> expandedEntities,
			List<Context> contexts, boolean localOnly, boolean doReplace,io.vertx.core.MultiMap headersFromReq) {
		Iterator<Map<String, Object>> itEntities = expandedEntities.iterator();
		Iterator<Context> itContext = contexts.iterator();
		Map<RemoteHost, List<Tuple2<Context, Map<String, Object>>>> remoteHost2Batch = Maps.newHashMap();
		List<Map<String, Object>> localEntities = Lists.newArrayList();
		while (itEntities.hasNext() && itContext.hasNext()) {
			Tuple2<Map<String, Object>, Collection<Tuple2<RemoteHost, Map<String, Object>>>> split = splitEntity(
					new UpsertEntityRequest(tenant, itEntities.next()));
			Map<String, Object> local = split.getItem1();
			Context context = itContext.next();
			if (local != null) {
				localEntities.add(local);
			} else {
				itContext.remove();
			}
			Collection<Tuple2<RemoteHost, Map<String, Object>>> remotes = split.getItem2();
			for (Tuple2<RemoteHost, Map<String, Object>> remote : remotes) {
				List<Tuple2<Context, Map<String, Object>>> entities2Context;
				if (remoteHost2Batch.containsKey(remote.getItem1())) {
					entities2Context = remoteHost2Batch.get(remote.getItem1());
				} else {
					entities2Context = Lists.newArrayList();
					remoteHost2Batch.put(remote.getItem1(), entities2Context);
				}
				entities2Context.add(Tuple2.of(context, remote.getItem2()));
			}
		}

		BatchRequest request = new BatchRequest(tenant, localEntities, contexts, AppConstants.UPSERT_REQUEST);
		Uni<List<NGSILDOperationResult>> local = entityDAO.batchUpsertEntity(request, doReplace).onItem().transform(dbResult -> {
			List<NGSILDOperationResult> result = Lists.newArrayList();
			List<Map<String, Object>> successes = (List<Map<String, Object>>) dbResult.get("success");
			List<Map<String, String>> fails = (List<Map<String, String>>) dbResult.get("failure");

			List<Map<String, Object>> olds = Lists.newArrayList();
			List<Map<String, Object>> news = Lists.newArrayList();
			for (Map<String, Object> entityResult : successes) {
				String entityId = (String) entityResult.get("id");
				boolean updated = (boolean) entityResult.get("updated");
				Map<String, Object> old = (Map<String, Object>) entityResult.get("old");
				Map<String, Object> newEntity = (Map<String, Object>) entityResult.get("new");
				olds.add(old);
				news.add(newEntity);
				NGSILDOperationResult opResult = new NGSILDOperationResult(AppConstants.UPSERT_REQUEST, entityId);
				opResult.setWasUpdated(updated);
				opResult.addSuccess(new CRUDSuccess(null, null, null, Sets.newHashSet()));
				result.add(opResult);
			}
			request.setBestCompleteResult(news);
			request.setPreviousEntity(olds);
			for (Map<String, String> fail : fails) {
				fail.entrySet().forEach(entry -> {
					String entityId = entry.getKey();
					String sqlstate = entry.getValue();
					request.removeFromPayloadAndContext(entityId);
					NGSILDOperationResult opResult = new NGSILDOperationResult(AppConstants.UPSERT_REQUEST, entityId);
					opResult.addFailure(new ResponseException(ErrorType.InvalidRequest, sqlstate));
					result.add(opResult);
				});

			}
			if (!request.getEntityIds().isEmpty()) {
				logger.debug("Upsert batch request sending to kafka " + request.getEntityIds());
				MicroServiceUtils.serializeAndSplitObjectAndEmit(request, messageSize, batchEmitter, objectMapper);
			}
			return result;
		});
		if (localOnly) {
			return local;
		}
		List<Uni<List<NGSILDOperationResult>>> unis = new ArrayList<>();

		for (Entry<RemoteHost, List<Tuple2<Context, Map<String, Object>>>> entry : remoteHost2Batch.entrySet()) {
			RemoteHost remoteHost = entry.getKey();
			MultiMap toFrwd = HttpUtils.getHeadToFrwd(remoteHost.headers(),headersFromReq);

			List<Tuple2<Context, Map<String, Object>>> tuples = entry.getValue();
			List<Uni<Map<String, Object>>> compactedUnis = Lists.newArrayList();
			for (Tuple2<Context, Map<String, Object>> tuple : tuples) {
				Map<String, Object> expanded = tuple.getItem2();
				Context context = tuple.getItem1();
				compactedUnis.add(jsonLdService.compact(expanded, null, context, AppConstants.opts, -1));
			}
			if (remoteHost.canDoBatchOp()) {
				unis.add(Uni.combine().all().unis(compactedUnis).combinedWith(list -> {
					List<Map<String, Object>> toSend = Lists.newArrayList();
					for (Object obj : list) {
						toSend.add((Map<String, Object>) obj);
					}
					return toSend;
				}).onItem().transformToUni(toSend -> {
					return webClient.postAbs(remoteHost.host() + NGSIConstants.ENDPOINT_BATCH_UPSERT)
							.putHeaders(toFrwd).sendJson(new JsonArray(toSend)).onItemOrFailure()
							.transform((response, failure) -> {
								return handleBatchResponse(response, failure, remoteHost, toSend,
										ArrayUtils.toArray(204));
							});
				}));
			} else {
				List<Uni<NGSILDOperationResult>> singleUnis = new ArrayList<>();
				for (Uni<Map<String, Object>> compactedUni : compactedUnis) {
					singleUnis.add(compactedUni.onItem().transformToUni(entity -> {
						return webClient
								.postAbs(remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "/"
										+ entity.get(NGSIConstants.JSON_LD_ID) + "/"
										+ NGSIConstants.QUERY_PARAMETER_ATTRS)
								.putHeaders(toFrwd).sendJsonObject(new JsonObject(entity))
								.onItemOrFailure().transformToUni((response, failure) -> {
									if (response.statusCode() == 404) {
										return webClient
												.postAbs(remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT)
												.putHeaders(toFrwd).sendJsonObject(new JsonObject(entity))
												.onItemOrFailure().transform((response1, failure1) -> {
													return HttpUtils.handleWebResponse(response1, failure1,
															ArrayUtils.toArray(201), remoteHost,
															AppConstants.CREATE_REQUEST,
															(String) entity.get(NGSIConstants.JSON_LD_ID),
															HttpUtils.getAttribsFromCompactedPayload(entity));

												});
									}
									return Uni.createFrom()
											.item(HttpUtils.handleWebResponse(response, failure,
													ArrayUtils.toArray(201), remoteHost, AppConstants.APPEND_REQUEST,
													(String) entity.get(NGSIConstants.JSON_LD_ID),
													HttpUtils.getAttribsFromCompactedPayload(entity)));
								});
					}));
				}
				unis.add(Uni.combine().all().unis(singleUnis).combinedWith(list -> {
					List<NGSILDOperationResult> result = Lists.newArrayList();
					list.forEach(obj -> result.add((NGSILDOperationResult) obj));
					return result;
				}));
			}
		}
		if (unis.isEmpty()) {
			return local;
		}
		unis.add(0, local);
		return Uni.combine().all().unis(unis).combinedWith(resultLists -> {
			List<NGSILDOperationResult> result = Lists.newArrayList();
			resultLists.forEach(resultList -> {
				result.addAll((List<NGSILDOperationResult>) resultList);
			});
			return result;
		});
	}

	public Uni<List<NGSILDOperationResult>> deleteBatch(String tenant, List<String> entityIds, boolean localOnly,io.vertx.core.MultiMap headersFromReq) {
		Map<RemoteHost, List<String>> host2Ids = Maps.newHashMap();
		for (String entityId : entityIds) {
			DeleteEntityRequest request = new DeleteEntityRequest(tenant, entityId);
			Set<RemoteHost> remoteHosts = getRemoteHostsForDelete(request);
			for (RemoteHost remoteHost : remoteHosts) {
				if (host2Ids.containsKey(remoteHost)) {
					host2Ids.get(remoteHost).add(entityId);
				} else {
					host2Ids.put(remoteHost, Lists.newArrayList(entityId));
				}
			}
		}
		List<Uni<List<NGSILDOperationResult>>> unis = new ArrayList<>(host2Ids.keySet().size());
		for (Entry<RemoteHost, List<String>> entry : host2Ids.entrySet()) {
			RemoteHost remoteHost = entry.getKey();
			MultiMap toFrwd = HttpUtils.getHeadToFrwd(remoteHost.headers(),headersFromReq);
			List<String> toSend = entry.getValue();
			if (remoteHost.canDoBatchOp()) {
				unis.add(webClient.postAbs(remoteHost.host() + NGSIConstants.ENDPOINT_BATCH_DELETE)
						.putHeaders(toFrwd).sendJson(new JsonArray(toSend)).onItemOrFailure()
						.transform((response, failure) -> {
							return handleBatchDeleteResponse(response, failure, remoteHost, toSend,
									ArrayUtils.toArray(204));
						}));
			} else {
				List<Uni<NGSILDOperationResult>> singleUnis = new ArrayList<>();
				for (String entityId : toSend) {
					singleUnis.add(webClient
							.deleteAbs(remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "/" + entityId)
							.putHeaders(toFrwd).send().onItemOrFailure()
							.transform((response, failure) -> {
								return HttpUtils.handleWebResponse(response, failure, ArrayUtils.toArray(201),
										remoteHost, AppConstants.CREATE_REQUEST, entityId, Sets.newHashSet());

							}));
				}
				unis.add(Uni.combine().all().unis(singleUnis).combinedWith(list -> {
					List<NGSILDOperationResult> result = Lists.newArrayList();
					list.forEach(obj -> result.add((NGSILDOperationResult) obj));
					return result;
				}));
			}
		}

		Uni<List<NGSILDOperationResult>> local = entityDAO.batchDeleteEntity(tenant, entityIds).onItem()
				.transform(dbResult -> {
					List<NGSILDOperationResult> result = Lists.newArrayList();
					List<Map<String, Object>> successes = (List<Map<String, Object>>) dbResult.get("success");
					List<Map<String, String>> fails = (List<Map<String, String>>) dbResult.get("failure");
					List<String> successEntityIds = Lists.newArrayList();
					List<Map<String, Object>> deleted = Lists.newArrayList();
					for (Map<String, Object> entry : successes) {
						String entityId = (String) entry.get("id");
						successEntityIds.add(entityId);
						deleted.add((Map<String, Object>) entry.get("old"));
						NGSILDOperationResult opResult = new NGSILDOperationResult(AppConstants.DELETE_REQUEST,
								entityId);
						opResult.addSuccess(new CRUDSuccess(null, null, null, Sets.newHashSet()));
						result.add(opResult);
					}
					for (Map<String, String> fail : fails) {
						fail.entrySet().forEach(entry -> {
							String entityId = entry.getKey();
							String sqlstate = entry.getValue();
							NGSILDOperationResult opResult = new NGSILDOperationResult(AppConstants.DELETE_REQUEST,
									entityId);
							opResult.addFailure(new ResponseException(ErrorType.NotFound, sqlstate));
							result.add(opResult);
						});

					}
					BatchRequest request = new BatchRequest(tenant, null, null, AppConstants.DELETE_REQUEST);
					request.setEntityIds(successEntityIds);
					request.setPreviousEntity(deleted);
					if (!request.getEntityIds().isEmpty()) {
						logger.debug("Delete batch request sending to kafka " + request.getEntityIds());
						MicroServiceUtils.serializeAndSplitObjectAndEmit(request, messageSize, batchEmitter,
								objectMapper);
					}
					return result;
				});

		unis.add(0, local);
		return Uni.combine().all().unis(unis).combinedWith(resultLists -> {
			List<NGSILDOperationResult> result = Lists.newArrayList();
			resultLists.forEach(resultList -> {
				result.addAll((List<NGSILDOperationResult>) resultList);
			});
			return result;
		});
	}

	public Uni<NGSILDOperationResult> mergePatch(String tenant, String entityId, Map<String, Object> resolved,
			Context context,io.vertx.core.MultiMap headersFromReq) {
		logger.debug("createMessage() :: started");
		MergePatchRequest request = new MergePatchRequest(tenant, entityId, resolved);
		Tuple2<Map<String, Object>, Collection<Tuple2<RemoteHost, Map<String, Object>>>> localAndRemote = splitEntity(
				request);
		Map<String, Object> localEntity = localAndRemote.getItem1();
		Collection<Tuple2<RemoteHost, Map<String, Object>>> remoteEntitiesAndHosts = localAndRemote.getItem2();
//		if (remoteEntitiesAndHosts.isEmpty()) {
//			request.setPayload(localEntity);
//			return localMergePatch(request, context);
//		}
		List<Uni<NGSILDOperationResult>> unis = new ArrayList<>(remoteEntitiesAndHosts.size());
		for (Tuple2<RemoteHost, Map<String, Object>> remoteEntityAndHost : remoteEntitiesAndHosts) {
			Map<String, Object> expanded = remoteEntityAndHost.getItem2();
			RemoteHost remoteHost = remoteEntityAndHost.getItem1();
			MultiMap toFrwd = HttpUtils.getHeadToFrwd(remoteHost.headers(),headersFromReq);

			if (remoteHost.canDoSingleOp()) {
				unis.add(prepareSplitUpEntityForSending(expanded, context).onItem().transformToUni(compacted -> {
					return webClient
							.patchAbs(remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "/" + entityId)
							.putHeaders(toFrwd).sendJsonObject(new JsonObject(compacted))
							.onItemOrFailure().transform((response, failure) -> {
								return HttpUtils.handleWebResponse(response, failure, ArrayUtils.toArray(201),
										remoteHost, AppConstants.CREATE_REQUEST, request.getId(),
										HttpUtils.getAttribsFromCompactedPayload(compacted));
							});
				}));
			} else {
				unis.add(prepareSplitUpEntityForSending(expanded, context).onItem().transformToUni(compacted -> {
					return webClient.post(remoteHost.host() + NGSIConstants.ENDPOINT_BATCH_CREATE)
							.putHeaders(toFrwd)
							.sendJson(new JsonArray(Lists.newArrayList(new JsonObject(compacted)))).onItemOrFailure()
							.transform((response, failure) -> {
								return handleBatchResponse(response, failure, remoteHost, Lists.newArrayList(compacted),
										ArrayUtils.toArray(201)).get(0);
							});
				}));
			}

		}
		if (localEntity != null && !localEntity.isEmpty()) {
			if (!unis.isEmpty() && isDifferentRemoteQueryAvailable(request, remoteEntitiesAndHosts)) {
				request.setDistributed(true);
			} else {
				request.setDistributed(false);
			}
			request.setBestCompleteResult(request.getPayload());
			request.setPayload(localEntity);
			unis.add(localMergePatch(request, context).onFailure().recoverWithItem(e -> {
				NGSILDOperationResult localResult = new NGSILDOperationResult(AppConstants.CREATE_REQUEST,
						request.getId());
				if (e instanceof ResponseException) {
					localResult.addFailure((ResponseException) e);
				} else {
					localResult.addFailure(new ResponseException(ErrorType.InternalError, e.getMessage()));
				}

				return localResult;

			}));
		}
		return Uni.combine().all().unis(unis).combinedWith(list -> {
			return getResult(list);
		});
	}

	private Uni<NGSILDOperationResult> localMergePatch(MergePatchRequest request, Context context) {
		return entityDAO.mergePatch(request).onItem().transform(v -> {
			request.setPreviousEntity(v.getItem1());
			request.setBestCompleteResult(v.getItem2());
			MicroServiceUtils.serializeAndSplitObjectAndEmit(request, messageSize, entityEmitter, objectMapper);
			NGSILDOperationResult localResult = new NGSILDOperationResult(AppConstants.MERGE_PATCH_REQUEST,
					request.getId());
			localResult.addSuccess(new CRUDSuccess(null, null, null, request.getPayload(), context));
			return localResult;
		});
	}

	public Uni<NGSILDOperationResult> replaceEntity(String tenant, Map<String, Object> resolved, Context context,io.vertx.core.MultiMap headersFromReq) {
		logger.debug("ReplaceMessage() :: started");

		ReplaceEntityRequest request = new ReplaceEntityRequest(tenant, resolved);
		Tuple2<Map<String, Object>, Collection<Tuple2<RemoteHost, Map<String, Object>>>> localAndRemote = splitEntity(
				request);
		Map<String, Object> localEntity = localAndRemote.getItem1();
		Collection<Tuple2<RemoteHost, Map<String, Object>>> remoteEntitiesAndHosts = localAndRemote.getItem2();
//		if (remoteEntitiesAndHosts.isEmpty()) {
//			request.setPayload(localEntity);
//			return replaceLocalEntity(request, context);
//		}
		List<Uni<NGSILDOperationResult>> unis = new ArrayList<>(remoteEntitiesAndHosts.size());
		for (Tuple2<RemoteHost, Map<String, Object>> remoteEntityAndHost : remoteEntitiesAndHosts) {
			Map<String, Object> expanded = remoteEntityAndHost.getItem2();
			RemoteHost remoteHost = remoteEntityAndHost.getItem1();
			MultiMap toFrwd = HttpUtils.getHeadToFrwd(remoteHost.headers(),headersFromReq);
			if (remoteHost.canDoSingleOp()) {
				unis.add(prepareSplitUpEntityForSending(expanded, context).onItem().transformToUni(compacted -> {
					return webClient
							.putAbs(remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "/" + request.getId())
							.putHeaders(toFrwd).sendJsonObject(new JsonObject(compacted))
							.onItemOrFailure().transform((response, failure) -> {
								return HttpUtils.handleWebResponse(response, failure, ArrayUtils.toArray(201),
										remoteHost, AppConstants.CREATE_REQUEST, request.getId(),
										HttpUtils.getAttribsFromCompactedPayload(compacted));
							});
				}));
			}

		}
		if (localEntity != null && !localEntity.isEmpty()) {
			if (!unis.isEmpty() && isDifferentRemoteQueryAvailable(request, remoteEntitiesAndHosts)) {
				request.setDistributed(true);
			} else {
				request.setDistributed(false);
			}
			request.setBestCompleteResult(request.getPayload());
			request.setPayload(localEntity);
			unis.add(replaceLocalEntity(request, context).onFailure().recoverWithItem(e -> {
				NGSILDOperationResult localResult = new NGSILDOperationResult(AppConstants.CREATE_REQUEST,
						request.getId());
				if (e instanceof ResponseException) {
					localResult.addFailure((ResponseException) e);
				} else {
					localResult.addFailure(new ResponseException(ErrorType.InternalError, e.getMessage()));
				}

				return localResult;

			}));
		}
		return Uni.combine().all().unis(unis).combinedWith(list -> {
			return getResult(list);
		});
	}

	private Uni<NGSILDOperationResult> replaceLocalEntity(ReplaceEntityRequest request, Context context) {
		return entityDAO.replaceEntity(request).onItem().transform(v -> {
			request.setPreviousEntity(v.getItem1());
			request.setBestCompleteResult(v.getItem2());
			MicroServiceUtils.serializeAndSplitObjectAndEmit(request, messageSize, entityEmitter, objectMapper);
			NGSILDOperationResult localResult = new NGSILDOperationResult(AppConstants.REPLACE_ENTITY_REQUEST,
					request.getId());
			localResult.addSuccess(new CRUDSuccess(null, null, null, request.getPayload(), context));
			return localResult;
		});
	}

	public Uni<NGSILDOperationResult> replaceAttribute(String tenant, Map<String, Object> resolved, Context context,
			String entityId, String attrId,io.vertx.core.MultiMap headersFromReq) {
		logger.debug("ReplaceMessage() :: started");
		if (!resolved.containsKey(attrId)) {
			if (resolved.size() == 1) {
				return Uni.createFrom().failure(new ResponseException(ErrorType.BadRequestData));
			}
			Map<String, Object> temp = new HashMap<>();
			temp.put(attrId, List.of(resolved));
			resolved = temp;
		}
		ReplaceAttribRequest request = new ReplaceAttribRequest(tenant, resolved, entityId, attrId);
		Tuple2<Map<String, Object>, Collection<Tuple2<RemoteHost, Map<String, Object>>>> localAndRemote = splitEntity(
				request);
		Map<String, Object> localEntity = localAndRemote.getItem1();
		Collection<Tuple2<RemoteHost, Map<String, Object>>> remoteEntitiesAndHosts = localAndRemote.getItem2();
		localEntity.remove(NGSIConstants.JSON_LD_TYPE);
		List<Uni<NGSILDOperationResult>> unis = new ArrayList<>(remoteEntitiesAndHosts.size());
//		if (remoteEntitiesAndHosts.isEmpty()) {
//			request.setPayload(localEntity);
//			return replaceLocalAttrib(request, context);
//		}
		for (Tuple2<RemoteHost, Map<String, Object>> remoteEntityAndHost : remoteEntitiesAndHosts) {
			Map<String, Object> expanded = remoteEntityAndHost.getItem2();
			RemoteHost remoteHost = remoteEntityAndHost.getItem1();
			MultiMap toFrwd = HttpUtils.getHeadToFrwd(remoteHost.headers(),headersFromReq);
			if (remoteHost.canDoSingleOp()) {
				unis.add(prepareSplitUpEntityForSending(expanded, context).onItem().transformToUni(compacted -> {
					return webClient
							.putAbs(remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "/" + entityId + "/"
									+ "attrs" + "/" + attrId)
							.putHeaders(remoteHost.headers()).sendJsonObject(new JsonObject(compacted))
							.onItemOrFailure().transform((response, failure) -> {
								return HttpUtils.handleWebResponse(response, failure, ArrayUtils.toArray(201),
										remoteHost, AppConstants.CREATE_REQUEST, request.getId(),
										HttpUtils.getAttribsFromCompactedPayload(compacted));
							});
				}));

			}
		}
		if (localEntity != null && !localEntity.isEmpty()) {
			if (!unis.isEmpty() && isDifferentRemoteQueryAvailable(request, remoteEntitiesAndHosts)) {
				request.setDistributed(true);
			} else {
				request.setDistributed(false);
			}
			request.setBestCompleteResult(request.getPayload());
			request.setPayload(localEntity);
			unis.add(replaceLocalAttrib(request, context).onFailure().recoverWithItem(e -> {
				NGSILDOperationResult localResult = new NGSILDOperationResult(AppConstants.CREATE_REQUEST,
						request.getId());
				if (e instanceof ResponseException) {
					localResult.addFailure((ResponseException) e);
				} else {
					localResult.addFailure(new ResponseException(ErrorType.InternalError, e.getMessage()));
				}

				return localResult;

			}));
		}
		return Uni.combine().all().unis(unis).combinedWith(list -> {
			return getResult(list);
		});
	}

	private Uni<NGSILDOperationResult> replaceLocalAttrib(ReplaceAttribRequest request, Context context) {
		return entityDAO.replaceAttrib(request).onItem().transform(v -> {
			request.setPreviousEntity(v.getItem1());
			request.setBestCompleteResult(v.getItem2());
			MicroServiceUtils.serializeAndSplitObjectAndEmit(request, messageSize, entityEmitter, objectMapper);
			NGSILDOperationResult localResult = new NGSILDOperationResult(AppConstants.REPLACE_ENTITY_REQUEST,
					request.getId());
			localResult.addSuccess(new CRUDSuccess(null, null, null, request.getPayload(), context));
			return localResult;
		});
	}

	private boolean isRemoteQueryPossible(Map<String, Object> payload, String tenant, String id) {

		Iterator<List<RegistrationEntry>> it = tenant2CId2QueryRegEntries.row(tenant).values().iterator();
		// ids, types, attrs, geo, scope

		List<String> types = null;
		if (payload.containsKey(NGSIConstants.JSON_LD_TYPE)) {
			types = (List<String>) payload.get(NGSIConstants.JSON_LD_TYPE);
		}

		while (it.hasNext()) {
			Iterator<RegistrationEntry> tenantRegs = it.next().iterator();
			while (tenantRegs.hasNext()) {

				RegistrationEntry regEntry = tenantRegs.next();
				if (regEntry.expiresAt() > 0 && regEntry.expiresAt() <= System.currentTimeMillis()) {
					it.remove();
					continue;
				}
				if ((((regEntry.eId() != null && regEntry.eId().equals(id))
						|| (regEntry.eIdp() != null && regEntry.eIdp().matches(id))
						|| (regEntry.eIdp() == null && regEntry.eId() == null)))
						&& (types == null || types.contains(regEntry.type()))) {
					return true;
				}
			}
		}
		return false;
	}


	public Uni<List<NGSILDOperationResult>> mergeBatch(String tenant, List<Map<String, Object>> expandedEntities,
														List<Context> contexts, boolean localOnly,boolean noOverWrite,io.vertx.core.MultiMap headersFromReq) {
		Iterator<Map<String, Object>> itEntities = expandedEntities.iterator();
		Iterator<Context> itContext = contexts.iterator();
		Map<RemoteHost, List<Tuple2<Context, Map<String, Object>>>> remoteHost2Batch = Maps.newHashMap();
		List<Map<String, Object>> localEntities = Lists.newArrayList();
		while (itEntities.hasNext() && itContext.hasNext()) {
			Map<String, Object> entity = itEntities.next();
			Tuple2<Map<String, Object>, Collection<Tuple2<RemoteHost, Map<String, Object>>>> split = splitEntity(
					new AppendEntityRequest(tenant, (String) entity.get(NGSIConstants.JSON_LD_ID), entity));
			Map<String, Object> local = split.getItem1();
			Context context = itContext.next();
			if (local != null) {
				local.remove(NGSIConstants.NGSI_LD_CREATED_AT);
				localEntities.add(local);
			} else {
				itContext.remove();
			}
			Collection<Tuple2<RemoteHost, Map<String, Object>>> remotes = split.getItem2();
			for (Tuple2<RemoteHost, Map<String, Object>> remote : remotes) {
				List<Tuple2<Context, Map<String, Object>>> entities2Context;
				if (remoteHost2Batch.containsKey(remote.getItem1())) {
					entities2Context = remoteHost2Batch.get(remote.getItem1());
				} else {
					entities2Context = Lists.newArrayList();
					remoteHost2Batch.put(remote.getItem1(), entities2Context);
				}
				entities2Context.add(Tuple2.of(context, remote.getItem2()));
			}
		}

		List<Uni<List<NGSILDOperationResult>>> unis = new ArrayList<>();
				if (!localOnly) {
			for (Entry<RemoteHost, List<Tuple2<Context, Map<String, Object>>>> entry : remoteHost2Batch.entrySet()) {
				RemoteHost remoteHost = entry.getKey();
				MultiMap toFrwd = HttpUtils.getHeadToFrwd(remoteHost.headers(),headersFromReq);
				List<Tuple2<Context, Map<String, Object>>> tuples = entry.getValue();
				List<Uni<Map<String, Object>>> compactedUnis = Lists.newArrayList();
				for (Tuple2<Context, Map<String, Object>> tuple : tuples) {
					Map<String, Object> expanded = tuple.getItem2();
					Context context = tuple.getItem1();
					compactedUnis.add(jsonLdService.compact(expanded, null, context, AppConstants.opts, -1));
				}
				if (remoteHost.canDoBatchOp()) {
					unis.add(Uni.combine().all().unis(compactedUnis).combinedWith(list -> {
						List<Map<String, Object>> toSend = Lists.newArrayList();
						for (Object obj : list) {
							toSend.add((Map<String, Object>) obj);
						}
						return toSend;
					}).onItem().transformToUni(toSend -> {
						return webClient.postAbs(remoteHost.host() + NGSIConstants.ENDPOINT_BATCH_MERGE)
								.putHeaders(toFrwd).sendJson(new JsonArray(toSend)).onItemOrFailure()
								.transform((response, failure) -> {
									return handleBatchResponse(response, failure, remoteHost, toSend,
											ArrayUtils.toArray(204));
								});
					}));
				} else {
					List<Uni<NGSILDOperationResult>> singleUnis = new ArrayList<>();
					for (Uni<Map<String, Object>> compactedUni : compactedUnis) {
						singleUnis.add(compactedUni.onItem().transformToUni(entity -> {
							return webClient
									.postAbs(remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "/"
											+ entity.get(NGSIConstants.JSON_LD_ID) + "/"
											+ NGSIConstants.QUERY_PARAMETER_ATTRS)
									.putHeaders(toFrwd).sendJsonObject(new JsonObject(entity))
									.onItemOrFailure().transform((response, failure) -> {
										return HttpUtils.handleWebResponse(response, failure, ArrayUtils.toArray(201),
												remoteHost, AppConstants.MERGE_PATCH_REQUEST,
												(String) entity.get(NGSIConstants.JSON_LD_ID),
												HttpUtils.getAttribsFromCompactedPayload(entity));
									});
						}));
					}
					unis.add(Uni.combine().all().unis(singleUnis).combinedWith(list -> {
						List<NGSILDOperationResult> result = Lists.newArrayList();
						list.forEach(obj -> result.add((NGSILDOperationResult) obj));
						return result;
					}));
				}
			}
		}
		if (!localEntities.isEmpty()) {
			BatchRequest request = new BatchRequest(tenant, localEntities, contexts, AppConstants.MERGE_PATCH_REQUEST);
			request.setNoOverwrite(noOverWrite);
			if (!unis.isEmpty() && isDifferentRemoteQueryAvailable(request, remoteHost2Batch)) {
				request.setDistributed(true);
			} else {
				request.setDistributed(false);
			}
			Uni<List<NGSILDOperationResult>> local = entityDAO.mergeBatchEntity(request).onItem()
					.transform(dbResult -> {
						List<NGSILDOperationResult> result = Lists.newArrayList();
						List<Map<String, Object>> successes = (List<Map<String, Object>>) dbResult.get("success");
						List<Map<String, String>> fails = (List<Map<String, String>>) dbResult.get("failure");
						for (Map<String, Object> success : successes) {
							String entityId = (String) success.get("id");
							NGSILDOperationResult opResult = new NGSILDOperationResult(AppConstants.MERGE_PATCH_REQUEST,
									entityId);
							opResult.addSuccess(new CRUDSuccess(null, null, null, Sets.newHashSet()));
							result.add(opResult);
						}
						for (Map<String, String> fail : fails) {
							fail.entrySet().forEach(entry -> {
								String entityId = entry.getKey();
								String sqlstate = entry.getValue();
								request.removeFromPayloadAndContext(entityId);
								NGSILDOperationResult opResult = new NGSILDOperationResult(AppConstants.MERGE_PATCH_REQUEST,
										entityId);
								if (sqlstate.equals(AppConstants.SQL_NOT_FOUND)) {
									opResult.addFailure(new ResponseException(ErrorType.NotFound, entityId));
								} else {
									opResult.addFailure(new ResponseException(ErrorType.InvalidRequest, sqlstate));
								}
								result.add(opResult);
							});

						}
						if (!request.getRequestPayload().isEmpty()) {
							logger.debug("Append batch request sending to kafka " + request.getEntityIds());
							MicroServiceUtils.serializeAndSplitObjectAndEmit(request, messageSize, batchEmitter,
									objectMapper);
						}
						return result;
					});

			unis.add(0, local);
		}
		if (unis.isEmpty()) {
			return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound));
		}
		return Uni.combine().all().unis(unis).combinedWith(resultLists -> {
			List<NGSILDOperationResult> result = Lists.newArrayList();
			resultLists.forEach(resultList -> {
				result.addAll((List<NGSILDOperationResult>) resultList);
			});
			return result;
		});
	}

}
