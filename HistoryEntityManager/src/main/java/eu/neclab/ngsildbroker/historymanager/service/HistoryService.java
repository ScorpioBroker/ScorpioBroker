package eu.neclab.ngsildbroker.historymanager.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Map.Entry;

import javax.annotation.PostConstruct;
import javax.el.MethodNotFoundException;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.commons.lang3.ArrayUtils;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.RemoteHost;
import eu.neclab.ngsildbroker.commons.datatypes.requests.AppendHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.HistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.UpdateHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.results.Attrib;
import eu.neclab.ngsildbroker.commons.datatypes.results.CRUDSuccess;

import eu.neclab.ngsildbroker.commons.datatypes.results.NGSILDOperationResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;

import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.EntryCRUDService;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.querybase.BaseQueryService;
import eu.neclab.ngsildbroker.commons.storage.StorageDAO;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.historymanager.repository.HistoryDAO;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.groups.UniAndGroup2;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.mutiny.tuples.Tuple4;
import io.smallrye.mutiny.unchecked.Unchecked;
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
public class HistoryService {

	private final static Logger logger = LoggerFactory.getLogger(HistoryService.class);

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

	private Random random = new Random();

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
			return handleDBCreateResult(request, resultTable, originalContext).onItem().transformToUni(tuple -> {
				NGSILDOperationResult localResult = tuple.getItem1();
				Map<RemoteHost, Map<String, Object>> remoteHosts = tuple.getItem2();
				List<Uni<NGSILDOperationResult>> unis = new ArrayList<>(remoteHosts.size());
				for (Entry<RemoteHost, Map<String, Object>> entry : remoteHosts.entrySet()) {
					RemoteHost remoteHost = entry.getKey();
					if (remoteHost.canDoSingleOp()) {
						unis.add(webClient.post(remoteHost.host() + NGSIConstants.NGSI_LD_TEMPORAL_ENTITIES_ENDPOINT)
								.putHeaders(remoteHost.headers()).sendJsonObject(new JsonObject(entry.getValue()))
								.onItemOrFailure().transform((response, failure) -> {
									return handleWebResponse(response, failure, ArrayUtils.toArray(201, 204),
											remoteHost, AppConstants.CREATE_REQUEST, request.getId(),
											HttpUtils.getAttribsFromCompactedPayload(entry.getValue()));

								}));
					} else {
						unis.add(webClient.post(remoteHost.host() + NGSIConstants.ENDPOINT_BATCH_CREATE)
								.putHeaders(remoteHost.headers())
								.sendJson(new JsonArray(Lists.newArrayList(new JsonObject(entry.getValue()))))
								.onItemOrFailure().transform((response, failure) -> {
									return handleBatchResponse(response, failure, remoteHost,
											Lists.newArrayList(entry.getValue()), ArrayUtils.toArray(201)).get(0);
								}));
					}
				}
				return Uni.combine().all().unis(unis).combinedWith(t -> localResult);
			});

		});
	}

	private void mergeRemoteResults(Map<String, NGSILDOperationResult> entityId2Result,
			List<NGSILDOperationResult> remoteResults) {
		for (NGSILDOperationResult remoteResult : remoteResults) {
			NGSILDOperationResult localResult = entityId2Result.get(remoteResult.getEntityId());
			if (localResult == null) {
				entityId2Result.put(remoteResult.getEntityId(), remoteResult);
			} else {
				localResult.getSuccesses().addAll(remoteResult.getSuccesses());
				localResult.getFailures().addAll(remoteResult.getFailures());
			}
		}
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

	public Uni<NGSILDOperationResult> appendToEntry(String tenant, String entityId, Map<String, Object> entry,
			Context originalContext) {
		AppendHistoryEntityRequest request = new AppendHistoryEntityRequest(tenant, entry, entityId, null); 
		return historyDAO.appendToHistoryEntity(request).onItem().transformToUni(resultTable -> {
			if (resultTable.size() == 0) {
				return Uni.createFrom().failure(new ResponseException(ErrorType.InternalError,
						"No result from the database this should never happen"));
			}
			return handleDBUpdateResult(request, resultTable, originalContext);
		})
		return null;
	}

	

	public Uni<NGSILDOperationResult> updateInstanceOfAttr(String tenant, String entityId, String attribId,
			String instanceId, Map<String, Object> payload, Context originalContext) {
		// TODO Auto-generated method stub
		return null;
	}

	public Uni<NGSILDOperationResult> deleteEntry(String tenant, String entityId, Context originalContext) {
		DeleteHistoryEntityRequest request = new DeleteHistoryEntityRequest(tenant, entityId, null);
		return historyDAO.deleteHistoryEntity(request).onItem().transformToUni(resultTable -> {
			if (resultTable.size() == 0) {
				return Uni.createFrom().failure(new ResponseException(ErrorType.InternalError,
						"No result from the database this should never happen"));
			}
			return handleDBDeleteResult(request, resultTable, originalContext);
		}).onItem().transformToUni(tuple -> {
			List<Uni<NGSILDOperationResult>> unis = Lists.newArrayList();
			for (RemoteHost remoteHost : tuple.getItem2()) {
				if (remoteHost.canDoSingleOp()) {
					unis.add(webClient
							.delete(remoteHost.host() + NGSIConstants.NGSI_LD_TEMPORAL_ENTITIES_ENDPOINT + "/"
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
			}

			return Uni.combine().all().unis(unis).combinedWith(list -> {
				NGSILDOperationResult result = tuple.getItem1();
				for (Object obj : list) {
					NGSILDOperationResult tmp = (NGSILDOperationResult) obj;
					result.getSuccesses().addAll(tmp.getSuccesses());
					result.getFailures().addAll(tmp.getFailures());
				}
				return result;
			});
		});
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

	public Uni<NGSILDOperationResult> deleteAttrFromEntry(String tenant, String entityId, String attrId,
			String datasetId, boolean deleteAll, Context originalContext) {

		return null;
	}

	public Uni<NGSILDOperationResult> deleteInstanceOfAttr(String tenant, String entityId, String attribId,
			String instanceId, Context originalContext) {
		// TODO Auto-generated method stub
		return null;
	}

	public Uni<Void> sendFail(BatchInfo batchInfo) {
		// no one is receiving anything history related at the moment
		// no subscriptions on history so this is empty for now but we leave it in since
		// this might change
		return Uni.createFrom().voidItem();
	}

	private Uni<Tuple2<NGSILDOperationResult, Map<RemoteHost, Map<String, Object>>>> handleDBCreateResult(
			CreateEntityRequest request, RowSet<Row> resultTable, Context originalContext) {
		NGSILDOperationResult operationResult = new NGSILDOperationResult(AppConstants.CREATE_REQUEST, request.getId());
		Map<RemoteHost, Map<String, Object>> remoteResults = Maps.newHashMap();
		List<Uni<Void>> unis = Lists.newArrayList();
		Map<Integer, Map<String, Object>> hash2Compacted = Maps.newHashMap();
		Context context = JsonLdProcessor.getCoreContextClone().parse(originalContext, true);
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
				MultiMap headers = MultiMap
						.newInstance(HttpUtils.getHeadersForRemoteCall((JsonArray) row.getJson(2), tenant));
				String cSourceId = row.getString(4);
				int hash = entityToForward.hashCode();
				Map<String, Object> compacted = hash2Compacted.get(hash);
				if (compacted == null) {
					try {
						compacted = JsonLdProcessor.compact(entityToForward, null, originalContext, AppConstants.opts,
								-1);
						compacted.remove("@context");
					} catch (ResponseException e) {
						// TODO add host info
						operationResult.addFailure(e);
					} catch (Exception e) {
						// TODO add host info
						operationResult.addFailure(new ResponseException(ErrorType.InternalError, e.getMessage()));
					}
					hash2Compacted.put(hash, compacted);
				}
				remoteResults.put(new RemoteHost(host, tenant, null, cSourceId, row.getBoolean(5), row.getBoolean(6)),
						compacted);
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
		return sendKafka.onItem().transform(v -> Tuple2.of(operationResult, remoteResults));
	}

	private Uni<Tuple2<NGSILDOperationResult, Set<RemoteHost>>> handleDBDeleteResult(DeleteEntityRequest request,
			RowSet<Row> resultTable, Context originalContext) {
		NGSILDOperationResult operationResult = new NGSILDOperationResult(AppConstants.DELETE_REQUEST, request.getId());
		Set<RemoteHost> remoteResults = new HashSet<>(resultTable.size());
		Context context = JsonLdProcessor.getCoreContextClone().parse(originalContext, true);
		RowIterator<Row> it = resultTable.iterator();
		Row row;
		Uni<Void> sendKafka = null;
		while (it.hasNext()) {
			row = it.next();
			switch (row.getString(0)) {
			case "ERROR": {
				String sqlState = row.getString(1);
				// TODO ERROR HANDLING
				if (request.getBatchInfo() != null && historyToKafkaEnabled) {
					sendKafka = sendFail(request.getBatchInfo());
				}
				break;
			}
			case "DELETED ENTITY": {
				Map<String, Object> entityDeleted = ((JsonObject) row.getJson(3)).getMap();
				request.setPayload(entityDeleted);
				operationResult.addSuccess(new CRUDSuccess(null, null, null, entityDeleted, context));
				if (historyToKafkaEnabled) {
					sendKafka = kafkaSenderInterface.send(request);
				}
				break;
			}
			default:
				String host = row.getString(0);
				String tenant = row.getString(1);
				Map<String, Object> entityToForward = ((JsonObject) row.getJson(3)).getMap();
				MultiMap headers = MultiMap
						.newInstance(HttpUtils.getHeadersForRemoteCall((JsonArray) row.getJson(2), tenant));
				String cSourceId = row.getString(4);
				remoteResults.add(new RemoteHost(host, tenant, null, cSourceId, row.getBoolean(5), row.getBoolean(6)));
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
		return sendKafka.onItem().transform(v -> Tuple2.of(operationResult, remoteResults));
	}
	
	private Uni<? extends NGSILDOperationResult> handleDBUpdateResult(AppendHistoryEntityRequest request,
			RowSet<Row> resultTable, Context originalContext) {
		// TODO Auto-generated method stub
		return null;
	}

	public void handleInternalCreate(String tenant, Map<String, Object> payload) {
		// TODO Auto-generated method stub

	}

}
