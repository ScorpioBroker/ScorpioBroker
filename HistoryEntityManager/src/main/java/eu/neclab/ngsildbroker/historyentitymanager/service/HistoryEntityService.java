package eu.neclab.ngsildbroker.historyentitymanager.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.apache.commons.lang3.ArrayUtils;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.locationtech.spatial4j.shape.Shape;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;
import eu.neclab.ngsildbroker.commons.datatypes.RegistrationEntry;
import eu.neclab.ngsildbroker.commons.datatypes.RemoteHost;
import eu.neclab.ngsildbroker.commons.datatypes.requests.AppendHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BatchRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteAttrHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteAttrInstanceHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteAttributeRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.EntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.UpdateAttrHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.results.Attrib;
import eu.neclab.ngsildbroker.commons.datatypes.results.CRUDSuccess;
import eu.neclab.ngsildbroker.commons.datatypes.results.NGSILDOperationResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import eu.neclab.ngsildbroker.historyentitymanager.repository.HistoryDAO;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.MultiMap;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.HttpResponse;
import io.vertx.mutiny.ext.web.client.WebClient;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowIterator;
import io.vertx.mutiny.sqlclient.RowSet;

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
	MutinyEmitter<BaseRequest> kafkaSenderInterface;

	@Inject
	Vertx vertx;

	WebClient webClient;

	private Table<String, String, RegistrationEntry> tenant2CId2RegEntries = HashBasedTable.create();

	@PostConstruct
	void init() {
		historyDAO.getAllRegistries().onItem().transform(t -> {
			tenant2CId2RegEntries = t;
			return null;
		}).await().indefinitely();
		webClient = WebClient.create(vertx);
	}

	public Uni<NGSILDOperationResult> createEntry(String tenant, Map<String, Object> resolved,
			Context originalContext) {
		logger.debug("createMessage() :: started");
		CreateHistoryEntityRequest request = new CreateHistoryEntityRequest(tenant, resolved, null);
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
			Map<String, Object> compacted;
			try {
				compacted = EntityTools.prepareSplitUpEntityForSending(expanded, originalContext);
			} catch (JsonLdError | ResponseException e) {
				logger.error("Failed to compact remote payload", e);
				continue;
			}
			RemoteHost remoteHost = remoteEntityAndHost.getItem1();

			unis.add(webClient.post(remoteHost.host() + NGSIConstants.NGSI_LD_TEMPORAL_ENTITIES_ENDPOINT)
					.putHeaders(remoteHost.headers()).sendJsonObject(new JsonObject(compacted)).onItemOrFailure()
					.transform((response, failure) -> {
						return HttpUtils.handleWebResponse(response, failure, ArrayUtils.toArray(201, 204), remoteHost,
								AppConstants.CREATE_REQUEST, request.getId(),
								HttpUtils.getAttribsFromCompactedPayload(compacted));

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

	public Uni<NGSILDOperationResult> appendToEntry(String tenant, String entityId,
			Map<String, Object> appendEntry, Context originalContext) {
		AppendHistoryEntityRequest request = new AppendHistoryEntityRequest(tenant, appendEntry, entityId, null);
		return historyDAO.appendToHistoryEntity(request).onItem().transform(v -> {
			NGSILDOperationResult result;
			result = new NGSILDOperationResult(AppConstants.APPEND_TEMPORAL_REQUEST, request.getId());
			result.addSuccess(new CRUDSuccess(null, null, null, appendEntry, originalContext));
			return result;
		});

	}

	public Uni<NGSILDOperationResult> updateInstanceOfAttr(String tenant, String entityId, String attribId,
			String instanceId, Map<String, Object> payload, Context originalContext) {
		UpdateAttrHistoryEntityRequest request = new UpdateAttrHistoryEntityRequest(tenant, entityId, attribId,
				instanceId, payload, null);
		return historyDAO.updateAttrInstanceInHistoryEntity(request).onItem().transform(v -> {
			NGSILDOperationResult result;
			result = new NGSILDOperationResult(AppConstants.UPDATE_TEMPORAL_INSTANCE_REQUEST, request.getId());
			result.addSuccess(new CRUDSuccess(null, null, null, payload, originalContext));
			return result;
		});
	}

	public Uni<NGSILDOperationResult> deleteEntry(String tenant, String entityId, Context originalContext) {
		DeleteHistoryEntityRequest request = new DeleteHistoryEntityRequest(tenant, entityId, null);
		return historyDAO.deleteHistoryEntity(request).onItem().transformToUni(resultTable -> {
			if (resultTable.size() == 0) {
				return Uni.createFrom().failure(new ResponseException(ErrorType.InternalError,
						"No result from the database this should never happen"));
			}
			return handleDBDeleteResult(request, resultTable, originalContext);
		});
	}

	public Uni<NGSILDOperationResult> deleteAttrFromEntry(String tenant, String entityId, String attrId,
			String datasetId, boolean deleteAll, Context originalContext) {
		DeleteAttrHistoryEntityRequest request = new DeleteAttrHistoryEntityRequest(tenant, entityId, attrId, datasetId,
				deleteAll, null);
		return historyDAO.deleteAttrFromHistoryEntity(request).onItem().transform(v -> {
			NGSILDOperationResult result;
			result = new NGSILDOperationResult(AppConstants.DELETE_TEMPORAL_ATTRIBUTE_REQUEST, request.getId());
			result.addSuccess(new CRUDSuccess(null, null, null, Sets.newHashSet(new Attrib(attrId, datasetId))));
			return result;
		});
	}

	public Uni<NGSILDOperationResult> deleteInstanceOfAttr(String tenant, String entityId, String attribId,
			String instanceId, Context originalContext) {
		DeleteAttrInstanceHistoryEntityRequest request = new DeleteAttrInstanceHistoryEntityRequest(tenant, entityId,
				attribId, instanceId, null);
		return historyDAO.deleteAttrInstanceInHistoryEntity(request).onItem().transform(v -> {
			NGSILDOperationResult result;
			result = new NGSILDOperationResult(AppConstants.DELETE_TEMPORAL_ATTRIBUTE_REQUEST, request.getId());
			result.addSuccess(new CRUDSuccess(null, null, null, Sets.newHashSet(new Attrib(attribId, instanceId))));
			return result;
		});
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
			return historyDAO.setAttributeDeleted((DeleteAttributeRequest) request).onFailure().recoverWithUni(e -> {
				logger.debug("Failed to record delete attrs", e);
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

	private Uni<NGSILDOperationResult> handleDBCreateResult(CreateHistoryEntityRequest request, RowSet<Row> resultTable,
			Context context) {
		NGSILDOperationResult operationResult = new NGSILDOperationResult(AppConstants.CREATE_REQUEST, request.getId());
		Map<Integer, Map<String, Object>> hash2Compacted = Maps.newHashMap();
		List<Uni<NGSILDOperationResult>> unis = new ArrayList<>(resultTable.size());
		RowIterator<Row> it = resultTable.iterator();
		Row row;
		Uni<Void> sendKafka = null;
		while (it.hasNext()) {
			row = it.next();
			switch (row.getString(0)) {
			case "ERROR": {
				String sqlState = row.getString(1);
				JsonObject entity = row.getJsonObject(3);
				if (sqlState.equals(AppConstants.SQL_ALREADY_EXISTS)) {
					operationResult.addFailure(new ResponseException(ErrorType.AlreadyExists,
							ErrorType.AlreadyExists.getTitle(), entity.getMap(), context));
				} else {
					operationResult.addFailure(new ResponseException(ErrorType.InternalError, row.getString(2)));
				}
				if (request.getBatchInfo() != null && historyToKafkaEnabled) {
					sendKafka = sendFail(request.getBatchInfo());
				}
				break;
			}
			case "ADDED ENTITY": {
				Map<String, Object> entityAdded = ((JsonObject) row.getJson(3)).getMap();
				request.setPayload(entityAdded);
				operationResult.addSuccess(new CRUDSuccess(null, null, null, entityAdded, context));
				if (historyToKafkaEnabled) {
					sendKafka = kafkaSenderInterface.send(request);
				}
				break;
			}
			case "UPDATED":
				operationResult.setWasUpdated(true);
				break;
			default:
				String host = row.getString(0);
				String tenant = row.getString(1);
				Map<String, Object> entityToForward = ((JsonObject) row.getJson(3)).getMap();
				String cSourceId = row.getString(4);
				int hash = entityToForward.hashCode();
				Map<String, Object> compacted;
				if (hash2Compacted.containsKey(hash)) {
					try {
						compacted = JsonLdProcessor.compact(entityToForward, null, context, AppConstants.opts, -1);
						compacted.remove("@context");
						hash2Compacted.put(hash, compacted);
					} catch (ResponseException e) {
						// TODO add host info
						operationResult.addFailure(e);
						break;
					} catch (Exception e) {
						// TODO add host info
						operationResult.addFailure(new ResponseException(ErrorType.InternalError, e.getMessage()));
						break;
					}
				} else {
					compacted = hash2Compacted.get(hash);
				}
				RemoteHost remoteHost = new RemoteHost(host, tenant,
						MultiMap.newInstance(HttpUtils.getHeadersForRemoteCall((JsonArray) row.getJson(2), tenant)),
						cSourceId, row.getBoolean(5), row.getBoolean(6));

				if (remoteHost.canDoSingleOp()) {
					unis.add(webClient.post(remoteHost.host() + NGSIConstants.NGSI_LD_TEMPORAL_ENTITIES_ENDPOINT)
							.putHeaders(remoteHost.headers()).sendJsonObject(new JsonObject(compacted))
							.onItemOrFailure().transform((response, failure) -> {
								return handleWebResponse(response, failure, ArrayUtils.toArray(201, 204), remoteHost,
										AppConstants.CREATE_REQUEST, request.getId(),
										HttpUtils.getAttribsFromCompactedPayload(compacted));

							}));
				} else {
					unis.add(webClient.post(remoteHost.host() + NGSIConstants.ENDPOINT_BATCH_CREATE)
							.putHeaders(remoteHost.headers())
							.sendJson(new JsonArray(Lists.newArrayList(new JsonObject(compacted)))).onItemOrFailure()
							.transform((response, failure) -> {
								return handleBatchResponse(response, failure, remoteHost, Lists.newArrayList(compacted),
										ArrayUtils.toArray(201)).get(0);
							}));
				}
				break;
			}

		}
		if (sendKafka == null) {
			if (request.getBatchInfo() != null && historyToKafkaEnabled) {
				sendKafka = sendFail(request.getBatchInfo());
			} else {
				sendKafka = Uni.createFrom().voidItem();
			}
		}
		return sendKafka.onItem().transformToUni(v -> {
			return Uni.combine().all().unis(unis).combinedWith(list -> {
				for (Object obj : list) {
					NGSILDOperationResult tmp = (NGSILDOperationResult) obj;
					operationResult.getSuccesses().addAll(tmp.getSuccesses());
					operationResult.getFailures().addAll(tmp.getFailures());
				}
				return operationResult;
			});
		});
	}

	private Uni<NGSILDOperationResult> handleDBDeleteResult(DeleteHistoryEntityRequest request, RowSet<Row> resultTable,
			Context originalContext) {
		NGSILDOperationResult operationResult = new NGSILDOperationResult(AppConstants.DELETE_REQUEST, request.getId());
		List<Uni<NGSILDOperationResult>> unis = Lists.newArrayList();
		RowIterator<Row> it = resultTable.iterator();
		Row row;
		Uni<Void> sendKafka = null;
		while (it.hasNext()) {
			row = it.next();

			switch (row.getString(0)) {
			case "ERROR": {
				// String sqlState = row.getString(1);
				// TODO ERROR HANDLING
				if (request.getBatchInfo() != null && historyToKafkaEnabled) {
					sendKafka = sendFail(request.getBatchInfo());
				}
				break;
			}
			case "DELETED ENTITY": {
				Map<String, Object> entityDeleted = ((JsonObject) row.getJson(3)).getMap();
				request.setPayload(entityDeleted);
				operationResult.addSuccess(new CRUDSuccess(null, null, null, entityDeleted, originalContext));
				if (historyToKafkaEnabled) {
					sendKafka = kafkaSenderInterface.send(request);
				}
				break;
			}
			default:
				MultiMap headers = MultiMap
						.newInstance(HttpUtils.getHeadersForRemoteCall((JsonArray) row.getJson(2), row.getString(1)));
				String cSourceId = row.getString(4);
				String host = row.getString(0);
				String tenant = row.getString(1);
				RemoteHost remoteHost = new RemoteHost(host, tenant, headers, cSourceId, row.getBoolean(5),
						row.getBoolean(6));
				String entityId = request.getId();
				if (row.getBoolean(5)) {
					unis.add(webClient
							.delete(row.getString(0) + NGSIConstants.NGSI_LD_TEMPORAL_ENTITIES_ENDPOINT + "/"
									+ entityId)
							.putHeaders(remoteHost.headers()).send().onItemOrFailure()
							.transform((response, failure) -> {
								return handleWebResponse(response, failure, ArrayUtils.toArray(204), remoteHost,
										AppConstants.DELETE_REQUEST, entityId, Sets.newHashSet());
							}));
				} else {
					unis.add(webClient.post(remoteHost.host() + NGSIConstants.ENDPOINT_TEMPORAL_BATCH_DELETE)
							.putHeaders(remoteHost.headers()).sendJson(new JsonArray(Lists.newArrayList(entityId)))
							.onItemOrFailure().transform((response, failure) -> {
								return handleBatchResponse(response, failure, remoteHost, Lists.newArrayList(),
										ArrayUtils.toArray(204)).get(0);

							}));
				}
				break;
			}

		}
		if (sendKafka == null) {
			if (request.getBatchInfo() != null && historyToKafkaEnabled) {
				sendKafka = sendFail(request.getBatchInfo());
			} else {
				sendKafka = Uni.createFrom().voidItem();
			}
		}
		return sendKafka.onItem().transformToUni(v -> {
			return Uni.combine().all().unis(unis).combinedWith(list -> {
				for (Object obj : list) {
					NGSILDOperationResult tmp = (NGSILDOperationResult) obj;
					operationResult.getSuccesses().addAll(tmp.getSuccesses());
					operationResult.getFailures().addAll(tmp.getFailures());
				}
				return operationResult;
			});
		});
	}

	private Uni<NGSILDOperationResult> handleDBAppendResult(AppendHistoryEntityRequest request, RowSet<Row> resultTable,
			Context context) {
		NGSILDOperationResult operationResult = new NGSILDOperationResult(AppConstants.APPEND_REQUEST, request.getId());
		Map<Integer, Map<String, Object>> hash2Compacted = Maps.newHashMap();
		List<Uni<NGSILDOperationResult>> unis = new ArrayList<>(resultTable.size());
		RowIterator<Row> it = resultTable.iterator();
		Row row;
		Uni<Void> sendKafka = null;
		while (it.hasNext()) {
			row = it.next();
			switch (row.getString(0)) {
			case "ERROR": {
				// JsonObject error = ((JsonObject) row.getJson(3));
				// for now the only "error" in sql would be if someone tries to remove a type
				// if this changes in sql we need to adapt here
				operationResult.addFailure(
						new ResponseException(ErrorType.BadRequestData, "You cannot remove a type from an entity"));
				if (request.getBatchInfo() != null && historyToKafkaEnabled) {
					sendKafka = sendFail(request.getBatchInfo());
				}
				break;
			}
			case "ADDED": {
				// [{"attribName": attribName, "datasetId": datasetId}]
				JsonArray addedAttribs = (JsonArray) row.getJson(3);
				Set<Attrib> attribs = Sets.newHashSet();
				addedAttribs.forEach(t -> {
					JsonObject obj = (JsonObject) t;
					attribs.add(new Attrib(obj.getString("attribName"), obj.getString("datasetId")));
				});
				operationResult.addSuccess(new CRUDSuccess(null, null, null, attribs));
				break;
			}
			case "NOT ADDED": {
				// [{"attribName": attribName, "datasetId": datasetId}]
				JsonArray addedAttribs = (JsonArray) row.getJson(3);
				Set<Attrib> attribs = Sets.newHashSet();
				addedAttribs.forEach(t -> {
					JsonObject obj = (JsonObject) t;
					attribs.add(new Attrib(obj.getString("attribName"), obj.getString("datasetId")));
				});
				operationResult.addFailure(new ResponseException(ErrorType.NotFound.getCode(),
						ErrorType.NotFound.getType(), ErrorType.NotFound.getTitle(), ErrorType.NotFound.getTitle(),
						null, null, null, attribs));
			}
			default:
				String host = row.getString(0);
				String tenant = row.getString(1);
				Map<String, Object> entityToForward = ((JsonObject) row.getJson(3)).getMap();
				String cSourceId = row.getString(4);
				int hash = entityToForward.hashCode();
				Map<String, Object> compacted;
				if (hash2Compacted.containsKey(hash)) {
					try {
						compacted = JsonLdProcessor.compact(entityToForward, null, context, AppConstants.opts, -1);
						compacted.remove("@context");
						hash2Compacted.put(hash, compacted);
					} catch (ResponseException e) {
						// TODO add host info
						operationResult.addFailure(e);
						break;
					} catch (Exception e) {
						// TODO add host info
						operationResult.addFailure(new ResponseException(ErrorType.InternalError, e.getMessage()));
						break;
					}
				} else {
					compacted = hash2Compacted.get(hash);
				}
				RemoteHost remoteHost = new RemoteHost(host, tenant,
						MultiMap.newInstance(HttpUtils.getHeadersForRemoteCall((JsonArray) row.getJson(2), tenant)),
						cSourceId, row.getBoolean(5), row.getBoolean(6));

				if (remoteHost.canDoSingleOp()) {
					unis.add(webClient
							.post(remoteHost.host() + NGSIConstants.NGSI_LD_TEMPORAL_ENTITIES_ENDPOINT + "/"
									+ request.getId() + "/attrs")
							.putHeaders(remoteHost.headers()).sendJsonObject(new JsonObject(compacted))
							.onItemOrFailure().transform((response, failure) -> {
								return handleWebResponse(response, failure, ArrayUtils.toArray(204), remoteHost,
										AppConstants.CREATE_REQUEST, request.getId(),
										HttpUtils.getAttribsFromCompactedPayload(compacted));

							}));
				} else {
					unis.add(webClient.post(remoteHost.host() + NGSIConstants.ENDPOINT_TEMPROAL_BATCH_APPEND)
							.putHeaders(remoteHost.headers())
							.sendJson(new JsonArray(Lists.newArrayList(new JsonObject(compacted)))).onItemOrFailure()
							.transform((response, failure) -> {
								return handleBatchResponse(response, failure, remoteHost, Lists.newArrayList(compacted),
										ArrayUtils.toArray(204)).get(0);
							}));
				}
				break;
			}

		}
		if (sendKafka == null) {
			if (request.getBatchInfo() != null && historyToKafkaEnabled) {
				if (operationResult.getFailures().isEmpty()) {
					sendKafka = kafkaSenderInterface.send(request);
				} else if (operationResult.getSuccesses().isEmpty()) {
					sendKafka = sendFail(request.getBatchInfo());
				} else {
					sendKafka = kafkaSenderInterface.send(getFilteredRequest(request, operationResult));
				}
			} else {
				sendKafka = Uni.createFrom().voidItem();
			}
		}
		return sendKafka.onItem().transformToUni(v -> {
			return Uni.combine().all().unis(unis).combinedWith(list -> {
				for (Object obj : list) {
					NGSILDOperationResult tmp = (NGSILDOperationResult) obj;
					operationResult.getSuccesses().addAll(tmp.getSuccesses());
					operationResult.getFailures().addAll(tmp.getFailures());
				}
				return operationResult;
			});
		});
	}

	private Uni<NGSILDOperationResult> handleDBInstanceUpdateResult(UpdateAttrHistoryEntityRequest request,
			RowSet<Row> resultTable, Context context) {
		// ('UPDATED ATTRS', null, null, LOCALENTITY, null, false, false)
		NGSILDOperationResult operationResult = new NGSILDOperationResult(AppConstants.UPDATE_TEMPORAL_INSTANCE_REQUEST,
				request.getId());
		Map<Integer, Map<String, Object>> hash2Compacted = Maps.newHashMap();
		List<Uni<NGSILDOperationResult>> unis = new ArrayList<>(resultTable.size());
		RowIterator<Row> it = resultTable.iterator();
		Row row;
		Uni<Void> sendKafka = null;
		while (it.hasNext()) {
			row = it.next();
			switch (row.getString(0)) {
			case "ERROR": {
				// JsonObject error = ((JsonObject) row.getJson(3));
				// for now the only "error" in sql would be if someone tries to remove a type
				// if this changes in sql we need to adapt here
				operationResult.addFailure(
						new ResponseException(ErrorType.BadRequestData, "You cannot remove a type from an entity"));
				if (request.getBatchInfo() != null && historyToKafkaEnabled) {
					sendKafka = sendFail(request.getBatchInfo());
				}
				break;
			}
			case "UPDATED ATTRS": {
				// full instance
				JsonObject addedInstance = row.getJsonObject(3);
				Set<Attrib> attribs = Sets.newHashSet(
						new Attrib(request.getAttrId(), addedInstance.getString(NGSIConstants.NGSI_LD_DATA_SET_ID)));
				operationResult.addSuccess(new CRUDSuccess(null, null, null, attribs));
				break;
			}
			default:
				String host = row.getString(0);
				String tenant = row.getString(1);
				Map<String, Object> entityToForward = ((JsonObject) row.getJson(3)).getMap();
				String cSourceId = row.getString(4);
				int hash = entityToForward.hashCode();
				Map<String, Object> compacted;
				if (hash2Compacted.containsKey(hash)) {
					try {
						compacted = JsonLdProcessor.compact(entityToForward, null, context, AppConstants.opts, -1);
						compacted.remove("@context");
						hash2Compacted.put(hash, compacted);
					} catch (ResponseException e) {
						// TODO add host info
						operationResult.addFailure(e);
						break;
					} catch (Exception e) {
						// TODO add host info
						operationResult.addFailure(new ResponseException(ErrorType.InternalError, e.getMessage()));
						break;
					}
				} else {
					compacted = hash2Compacted.get(hash);
				}
				RemoteHost remoteHost = new RemoteHost(host, tenant,
						MultiMap.newInstance(HttpUtils.getHeadersForRemoteCall((JsonArray) row.getJson(2), tenant)),
						cSourceId, row.getBoolean(5), row.getBoolean(6));

				if (remoteHost.canDoSingleOp()) {
					unis.add(webClient
							.post(remoteHost.host() + NGSIConstants.NGSI_LD_TEMPORAL_ENTITIES_ENDPOINT + "/"
									+ request.getId() + "/attrs/" + request.getAttrId() + "/" + request.getInstanceId())
							.putHeaders(remoteHost.headers()).sendJsonObject(new JsonObject(compacted))
							.onItemOrFailure().transform((response, failure) -> {
								return handleWebResponse(response, failure, ArrayUtils.toArray(204), remoteHost,
										AppConstants.UPDATE_TEMPORAL_INSTANCE_REQUEST, request.getId(),
										HttpUtils.getAttribsFromCompactedPayload(compacted));

							}));
				}
				// TODO clear up with CIM to add batch requests for this
//				else {
//					unis.add(webClient.post(remoteHost.host() + NGSIConstants.ENDPOINT_TEMPROAL_BATCH_APPEND)
//							.putHeaders(remoteHost.headers())
//							.sendJson(new JsonArray(Lists.newArrayList(new JsonObject(compacted)))).onItemOrFailure()
//							.transform((response, failure) -> {
//								return handleBatchResponse(response, failure, remoteHost, Lists.newArrayList(compacted),
//										ArrayUtils.toArray(204)).get(0);
//							}));
//				}
				break;
			}

		}
		if (sendKafka == null) {
			if (request.getBatchInfo() != null && historyToKafkaEnabled) {
				if (operationResult.getFailures().isEmpty()) {
					sendKafka = kafkaSenderInterface.send(request);
				} else {
					sendKafka = sendFail(request.getBatchInfo());
				}
			} else {
				sendKafka = Uni.createFrom().voidItem();
			}
		}
		return sendKafka.onItem().transformToUni(v -> {
			return Uni.combine().all().unis(unis).combinedWith(list -> {
				for (Object obj : list) {
					NGSILDOperationResult tmp = (NGSILDOperationResult) obj;
					operationResult.getSuccesses().addAll(tmp.getSuccesses());
					operationResult.getFailures().addAll(tmp.getFailures());
				}
				return operationResult;
			});
		});
	}

	private Uni<NGSILDOperationResult> handleDBDeleteAttrResult(DeleteAttrHistoryEntityRequest request,
			RowSet<Row> resultTable, Context context) {
		// INSERT INTO resultTable VALUES ('DELETED ATTRS', null, null, LOCALATTRS,
		// null, false, false);
		NGSILDOperationResult operationResult = new NGSILDOperationResult(
				AppConstants.DELETE_TEMPORAL_ATTRIBUTE_REQUEST, request.getId());
		List<Uni<NGSILDOperationResult>> unis = new ArrayList<>(resultTable.size());
		RowIterator<Row> it = resultTable.iterator();
		Row row;
		Uni<Void> sendKafka = null;
		while (it.hasNext()) {
			row = it.next();
			switch (row.getString(0)) {
			case "ERROR": {
				// TODO update error handling
				operationResult.addFailure(
						new ResponseException(ErrorType.BadRequestData, "You cannot remove a type from an entity"));
				if (request.getBatchInfo() != null && historyToKafkaEnabled) {
					sendKafka = sendFail(request.getBatchInfo());
				}
				break;
			}
			case "DELETED ATTRS": {
				// full instance
				Set<Attrib> attribs = Sets.newHashSet(new Attrib(request.getAttribName(), request.getDatasetId()));
				operationResult.addSuccess(new CRUDSuccess(null, null, null, attribs));
				break;
			}
			default:
				String host = row.getString(0);
				String tenant = row.getString(1);
				String cSourceId = row.getString(4);

				RemoteHost remoteHost = new RemoteHost(host, tenant,
						MultiMap.newInstance(HttpUtils.getHeadersForRemoteCall((JsonArray) row.getJson(2), tenant)),
						cSourceId, row.getBoolean(5), row.getBoolean(6));

				if (remoteHost.canDoSingleOp()) {
					int builderSize = remoteHost.host().length()
							+ NGSIConstants.NGSI_LD_TEMPORAL_ENTITIES_ENDPOINT.length() + request.getId().length()
							+ request.getAttribName().length() + 60; // explaination for 60 it's 9 from the attrs and
																		// slashes and than datasetid or deleteall

					StringBuilder url = new StringBuilder(builderSize);
					url.append(remoteHost.host());
					url.append(NGSIConstants.NGSI_LD_TEMPORAL_ENTITIES_ENDPOINT + "/");
					url.append(request.getId());
					url.append("/attrs/" + request.getAttribName());
					if (request.isDeleteAll()) {
						url.append("?deleteAll=1");
					} else {
						if (request.getDatasetId() != null) {
							url.append("?datasetId=" + request.getDatasetId());
						}
					}
					unis.add(webClient.delete(url.toString()).putHeaders(remoteHost.headers()).send().onItemOrFailure()
							.transform((response, failure) -> {
								return handleWebResponse(response, failure, ArrayUtils.toArray(204), remoteHost,
										AppConstants.DELETE_TEMPORAL_ATTRIBUTE_REQUEST, request.getId(),
										Sets.newHashSet(new Attrib(request.getAttribName(), request.getDatasetId())));

							}));
				}
				// TODO clear up with CIM to add batch requests for this
//				else {
//					unis.add(webClient.post(remoteHost.host() + NGSIConstants.ENDPOINT_TEMPROAL_BATCH_APPEND)
//							.putHeaders(remoteHost.headers())
//							.sendJson(new JsonArray(Lists.newArrayList(new JsonObject(compacted)))).onItemOrFailure()
//							.transform((response, failure) -> {
//								return handleBatchResponse(response, failure, remoteHost, Lists.newArrayList(compacted),
//										ArrayUtils.toArray(204)).get(0);
//							}));
//				}
				break;
			}

		}
		if (sendKafka == null) {
			if (request.getBatchInfo() != null && historyToKafkaEnabled) {
				if (operationResult.getFailures().isEmpty()) {
					sendKafka = kafkaSenderInterface.send(request);
				} else {
					sendKafka = sendFail(request.getBatchInfo());
				}
			} else {
				sendKafka = Uni.createFrom().voidItem();
			}
		}
		return sendKafka.onItem().transformToUni(v -> {
			return Uni.combine().all().unis(unis).combinedWith(list -> {
				for (Object obj : list) {
					NGSILDOperationResult tmp = (NGSILDOperationResult) obj;
					operationResult.getSuccesses().addAll(tmp.getSuccesses());
					operationResult.getFailures().addAll(tmp.getFailures());
				}
				return operationResult;
			});
		});
	}

	private Uni<NGSILDOperationResult> handleDBInstanceDeleteResult(DeleteAttrInstanceHistoryEntityRequest request,
			RowSet<Row> resultTable, Context originalContext) {
		// ('DELETED ATTRS', null, null, LOCALINSTANCE, null, false, false)
		NGSILDOperationResult operationResult = new NGSILDOperationResult(
				AppConstants.DELETE_TEMPORAL_ATTRIBUTE_REQUEST, request.getId());
		List<Uni<NGSILDOperationResult>> unis = new ArrayList<>(resultTable.size());
		RowIterator<Row> it = resultTable.iterator();
		Row row;
		Uni<Void> sendKafka = null;
		while (it.hasNext()) {
			row = it.next();
			switch (row.getString(0)) {
			case "ERROR": {
				// TODO update error handling
				operationResult.addFailure(
						new ResponseException(ErrorType.BadRequestData, "You cannot remove a type from an entity"));
				if (request.getBatchInfo() != null && historyToKafkaEnabled) {
					sendKafka = sendFail(request.getBatchInfo());
				}
				break;
			}
			case "DELETED ATTRS": {
				// full instance
				Set<Attrib> attribs = Sets.newHashSet(new Attrib(request.getAttrId(), request.getInstanceId()));
				operationResult.addSuccess(new CRUDSuccess(null, null, null, attribs));
				break;
			}
			default:
				String host = row.getString(0);
				String tenant = row.getString(1);
				String cSourceId = row.getString(4);

				RemoteHost remoteHost = new RemoteHost(host, tenant,
						MultiMap.newInstance(HttpUtils.getHeadersForRemoteCall((JsonArray) row.getJson(2), tenant)),
						cSourceId, row.getBoolean(5), row.getBoolean(6));

				if (remoteHost.canDoSingleOp()) {
					int builderSize = remoteHost.host().length()
							+ NGSIConstants.NGSI_LD_TEMPORAL_ENTITIES_ENDPOINT.length() + request.getId().length()
							+ request.getAttrId().length() + request.getInstanceId().length() + 12; // explaination for
																									// 12 is from the
																									// attrs and
																									// slashes
					StringBuilder url = new StringBuilder(builderSize);
					url.append(remoteHost.host());
					url.append(NGSIConstants.NGSI_LD_TEMPORAL_ENTITIES_ENDPOINT + "/");
					url.append(request.getId());
					url.append("/attrs/" + request.getAttrId());
					url.append("/" + request.getInstanceId());
					unis.add(webClient.delete(url.toString()).putHeaders(remoteHost.headers()).send().onItemOrFailure()
							.transform((response, failure) -> {
								return handleWebResponse(response, failure, ArrayUtils.toArray(204), remoteHost,
										AppConstants.DELETE_TEMPORAL_ATTRIBUTE_REQUEST, request.getId(),
										Sets.newHashSet(new Attrib(request.getAttrId(), request.getInstanceId())));

							}));
				}
				// TODO clear up with CIM to add batch requests for this
//				else {
//					unis.add(webClient.post(remoteHost.host() + NGSIConstants.ENDPOINT_TEMPROAL_BATCH_APPEND)
//							.putHeaders(remoteHost.headers())
//							.sendJson(new JsonArray(Lists.newArrayList(new JsonObject(compacted)))).onItemOrFailure()
//							.transform((response, failure) -> {
//								return handleBatchResponse(response, failure, remoteHost, Lists.newArrayList(compacted),
//										ArrayUtils.toArray(204)).get(0);
//							}));
//				}
				break;
			}

		}
		if (sendKafka == null) {
			if (request.getBatchInfo() != null && historyToKafkaEnabled) {
				if (operationResult.getFailures().isEmpty()) {
					sendKafka = kafkaSenderInterface.send(request);
				} else {
					sendKafka = sendFail(request.getBatchInfo());
				}
			} else {
				sendKafka = Uni.createFrom().voidItem();
			}
		}
		return sendKafka.onItem().transformToUni(v -> {
			return Uni.combine().all().unis(unis).combinedWith(list -> {
				for (Object obj : list) {
					NGSILDOperationResult tmp = (NGSILDOperationResult) obj;
					operationResult.getSuccesses().addAll(tmp.getSuccesses());
					operationResult.getFailures().addAll(tmp.getFailures());
				}
				return operationResult;
			});
		});
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

	private NGSILDOperationResult handleWebResponse(HttpResponse<Buffer> response, Throwable failure,
			Integer[] integers, RemoteHost remoteHost, int operationType, String entityId, Set<Attrib> attrs) {

		NGSILDOperationResult result = new NGSILDOperationResult(operationType, entityId);
		if (failure != null) {
			result.addFailure(new ResponseException(ErrorType.InternalError, failure.getMessage(), remoteHost, attrs));
		} else {
			int statusCode = response.statusCode();
			if (ArrayUtils.contains(integers, statusCode)) {
				result.addSuccess(new CRUDSuccess(remoteHost, attrs));
			} else if (statusCode == 207) {
				JsonObject jsonObj = response.bodyAsJsonObject();
				if (jsonObj != null) {
					NGSILDOperationResult remoteResult;
					try {
						remoteResult = NGSILDOperationResult.getFromPayload(jsonObj.getMap());
					} catch (ResponseException e) {
						result.addFailure(e);
						return result;
					}
					result.getFailures().addAll(remoteResult.getFailures());
					result.getSuccesses().addAll(remoteResult.getSuccesses());
				}

			} else {

				JsonObject responseBody = response.bodyAsJsonObject();
				if (responseBody == null) {
					result.addFailure(new ResponseException(500, NGSIConstants.ERROR_UNEXPECTED_RESULT,
							NGSIConstants.ERROR_UNEXPECTED_RESULT_NULL_TITLE, statusCode, remoteHost, attrs));

				} else {
					if (!responseBody.containsKey(NGSIConstants.ERROR_TYPE)
							|| !responseBody.containsKey(NGSIConstants.ERROR_TITLE)
							|| !responseBody.containsKey(NGSIConstants.ERROR_DETAIL)) {
						result.addFailure(
								new ResponseException(statusCode, responseBody.getString(NGSIConstants.ERROR_TYPE),
										responseBody.getString(NGSIConstants.ERROR_TITLE),
										responseBody.getMap().get(NGSIConstants.ERROR_DETAIL), remoteHost, attrs));
					} else {
						result.addFailure(new ResponseException(500, NGSIConstants.ERROR_UNEXPECTED_RESULT,
								NGSIConstants.ERROR_UNEXPECTED_RESULT_NOT_EXPECTED_BODY_TITLE, responseBody.getMap(),
								remoteHost, attrs));
					}
				}
			}
		}
		return result;
	}

	private Tuple2<Map<String, Object>, Collection<Tuple2<RemoteHost, Map<String, Object>>>> splitEntity(
			EntityRequest request) {
		Map<String, Object> originalEntity = request.getPayload();
		Collection<RegistrationEntry> regs = tenant2CId2RegEntries.row(request.getTenant()).values();
		Iterator<RegistrationEntry> it = regs.iterator();
		Object originalScopes = originalEntity.remove(NGSIConstants.NGSI_LD_SCOPE);
		String entityId = (String) originalEntity.remove(NGSIConstants.JSON_LD_ID);
		List<String> originalTypes = (List<String>) originalEntity.remove(NGSIConstants.JSON_LD_TYPE);
		Map<String, Tuple2<RemoteHost, Map<String, Object>>> cId2RemoteHostEntity = Maps.newHashMap();
		Shape location = null;
		Set<String> toBeRemoved = Sets.newHashSet();
		for (Entry<String, Object> entry : originalEntity.entrySet()) {
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
					matches = regEntry.matches(entityId, originalTypes, null, entry.getKey(), originalScopes, location);
				} else {
					matches = regEntry.matches(entityId, originalTypes, entry.getKey(), null, originalScopes, location);
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
									regHost.cSourceId(), regEntry.upsertTemporal(), false);
							break;
						case AppConstants.APPEND_TEMPORAL_REQUEST:
							host = new RemoteHost(regHost.host(), regHost.tenant(), regHost.headers(),
									regHost.cSourceId(), regEntry.appendAttrsTemporal(), false);
							break;
						case AppConstants.UPDATE_TEMPORAL_INSTANCE_REQUEST:
							host = new RemoteHost(regHost.host(), regHost.tenant(), regHost.headers(),
									regHost.cSourceId(), regEntry.updateAttrsTemporal(), false);
							break;
						case AppConstants.DELETE_TEMPORAL_REQUEST:
							host = new RemoteHost(regHost.host(), regHost.tenant(), regHost.headers(),
									regHost.cSourceId(), regEntry.deleteTemporal(), false);
							break;
						case AppConstants.DELETE_TEMPORAL_ATTRIBUTE_REQUEST:
							host = new RemoteHost(regHost.host(), regHost.tenant(), regHost.headers(),
									regHost.cSourceId(), regEntry.deleteAttrsTemporal(), false);
							break;
						case AppConstants.DELETE_TEMPORAL_ATTRIBUTE_INSTANCE_REQUEST:
							host = new RemoteHost(regHost.host(), regHost.tenant(), regHost.headers(),
									regHost.cSourceId(), regEntry.deleteAttrInstanceTemporal(), false);
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

	private BaseRequest getFilteredRequest(AppendHistoryEntityRequest request, NGSILDOperationResult operationResult) {
		// +2 for id and type
		Map<String, Object> filteredCopy = new HashMap<>(operationResult.getSuccesses().size() + 2);
		// there is only one success for added
		Set<Attrib> attribs = operationResult.getSuccesses().get(0).getAttribs();
		Map<String, Object> originalRequest = request.getPayload();
		for (Attrib attrib : attribs) {
			String attribName = attrib.getAttribName();
			filteredCopy.put(attribName, originalRequest.get(attribName));
		}
		filteredCopy.put(NGSIConstants.JSON_LD_ID, request.getId());
		if (originalRequest.containsKey(NGSIConstants.JSON_LD_TYPE)) {
			filteredCopy.put(NGSIConstants.JSON_LD_TYPE, originalRequest.get(NGSIConstants.JSON_LD_TYPE));
		}
		return new AppendHistoryEntityRequest(request.getTenant(), filteredCopy, request.getId(),
				request.getBatchInfo());
	}

	private Uni<Void> sendFail(BatchInfo batchInfo) {
		// no one is receiving anything history related at the moment
		// no subscriptions on history so this is empty for now but we leave it in since
		// this might change
		return Uni.createFrom().voidItem();
	}

}
