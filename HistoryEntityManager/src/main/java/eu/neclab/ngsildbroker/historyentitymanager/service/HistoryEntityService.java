package eu.neclab.ngsildbroker.historyentitymanager.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.commons.lang3.ArrayUtils;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;
import eu.neclab.ngsildbroker.commons.datatypes.RemoteHost;
import eu.neclab.ngsildbroker.commons.datatypes.requests.AppendHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteAttrHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteAttrInstanceHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.UpdateAttrHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.results.Attrib;
import eu.neclab.ngsildbroker.commons.datatypes.results.CRUDSuccess;
import eu.neclab.ngsildbroker.commons.datatypes.results.NGSILDOperationResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.historyentitymanager.repository.HistoryDAO;
import io.smallrye.mutiny.Uni;
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

@Singleton
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

	@PostConstruct
	void init() {
		webClient = WebClient.create(vertx);
	}

	public Uni<NGSILDOperationResult> createEntry(String tenant, Map<String, Object> resolved,
			Context originalContext) {
		logger.debug("createMessage() :: started");
		CreateHistoryEntityRequest request = new CreateHistoryEntityRequest(tenant, resolved, null);
		return historyDAO.createHistoryEntity(request).onItem().transformToUni(resultTable -> {
			if (resultTable.size() == 0) {
				return Uni.createFrom().failure(new ResponseException(ErrorType.InternalError,
						"No result from the database this should never happen"));
			}
			return handleDBCreateResult(request, resultTable, originalContext);

		});
	}

	public Uni<NGSILDOperationResult> appendToEntry(String tenant, String entityId, Map<String, Object> appendEntry,
			Context originalContext) {
		AppendHistoryEntityRequest request = new AppendHistoryEntityRequest(tenant, appendEntry, entityId, null);
		return historyDAO.appendToHistoryEntity(request).onItem().transformToUni(resultTable -> {
			if (resultTable.size() == 0) {
				return Uni.createFrom().failure(new ResponseException(ErrorType.InternalError,
						"No result from the database this should never happen"));
			}
			return handleDBAppendResult(request, resultTable, originalContext);
		});

	}

	public Uni<NGSILDOperationResult> updateInstanceOfAttr(String tenant, String entityId, String attribId,
			String instanceId, Map<String, Object> payload, Context originalContext) {
		UpdateAttrHistoryEntityRequest request = new UpdateAttrHistoryEntityRequest(tenant, entityId, attribId,
				instanceId, payload, null);
		return historyDAO.updateAttrInstanceInHistoryEntity(request).onItem().transformToUni(resultTable -> {
			if (resultTable.size() == 0) {
				return Uni.createFrom().failure(new ResponseException(ErrorType.InternalError,
						"No result from the database this should never happen"));
			}
			return handleDBInstanceUpdateResult(request, resultTable, originalContext);
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
		return historyDAO.deleteAttrFromHistoryEntity(request).onItem().transformToUni(resultTable -> {
			if (resultTable.size() == 0) {
				return Uni.createFrom().failure(new ResponseException(ErrorType.InternalError,
						"No result from the database this should never happen"));
			}
			return handleDBDeleteAttrResult(request, resultTable, originalContext);
		});
	}

	public Uni<NGSILDOperationResult> deleteInstanceOfAttr(String tenant, String entityId, String attribId,
			String instanceId, Context originalContext) {
		DeleteAttrInstanceHistoryEntityRequest request = new DeleteAttrInstanceHistoryEntityRequest(tenant, entityId,
				attribId, instanceId, null);
		return historyDAO.deleteAttrInstanceInHistoryEntity(request).onItem().transformToUni(resultTable -> {
			if (resultTable.size() == 0) {
				return Uni.createFrom().failure(new ResponseException(ErrorType.InternalError,
						"No result from the database this should never happen"));
			}
			return handleDBInstanceDeleteResult(request, resultTable, originalContext);
		});
	}

	public void handleInternalRequest(BaseRequest request) {

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
			RowSet<Row> resultTable, Context originalContext) {
		// INSERT INTO resultTable VALUES ('DELETED ATTRS', null, null, LOCALATTRS,
		// null, false, false);
		return null;
	}

	private Uni<NGSILDOperationResult> handleDBInstanceDeleteResult(DeleteAttrInstanceHistoryEntityRequest request,
			RowSet<Row> resultTable, Context originalContext) {
		// ('DELETED ATTRS', null, null, LOCALINSTANCE, null, false, false)

		return null;
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
