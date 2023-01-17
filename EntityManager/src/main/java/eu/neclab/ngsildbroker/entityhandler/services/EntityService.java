package eu.neclab.ngsildbroker.entityhandler.services;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.collect.Tables;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;
import eu.neclab.ngsildbroker.commons.datatypes.RemoteHost;
import eu.neclab.ngsildbroker.commons.datatypes.requests.*;
import eu.neclab.ngsildbroker.commons.datatypes.results.Attrib;
import eu.neclab.ngsildbroker.commons.datatypes.results.CRUDSuccess;
import eu.neclab.ngsildbroker.commons.datatypes.results.NGSILDOperationResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.mutiny.tuples.Tuple3;
import io.smallrye.mutiny.tuples.Tuple4;
import io.smallrye.mutiny.tuples.Tuple6;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.annotations.Broadcast;
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
import org.apache.commons.lang3.ArrayUtils;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.locationtech.spatial4j.shape.Shape;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

@Singleton
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
	MutinyEmitter<BaseRequest> kafkaSenderInterface;

	@Inject
	Vertx vertx;

	WebClient webClient;

	private Table<String, String, RegistrationEntry> tenant2CId2RegEntries = HashBasedTable.create();

	private JsonLdOptions opts = new JsonLdOptions(JsonLdOptions.JSON_LD_1_1);

	private Random random = new Random();

	@PostConstruct
	void init() {
		webClient = WebClient.create(vertx);
		entityDAO.getAllRegistries().onItem().transform(t -> {
			tenant2CId2RegEntries = t;
			return null;
		}).await().indefinitely();
	}

	public Uni<NGSILDOperationResult> createEntry(String tenant, Map<String, Object> resolved,
			Context originalContext) {
		logger.debug("createMessage() :: started");
		CreateEntityRequest request = new CreateEntityRequest(tenant, resolved, null);
		return entityDAO.createEntity(request).onItem().transformToUni(resultTable -> {
			if (resultTable.size() == 0) {
				return Uni.createFrom().failure(new ResponseException(ErrorType.InternalError,
						"No result from the database this should never happen"));
			}
			return handleDBCreateResult(request, resultTable, originalContext).onItem().transformToUni(tuple -> {
				NGSILDOperationResult localResult = tuple.getItem1();
				Map<RemoteHost, Map<String, Object>> remoteHosts = tuple.getItem2();
				List<Uni<Void>> unis = new ArrayList<>(remoteHosts.size());
				for (Entry<RemoteHost, Map<String, Object>> entry : remoteHosts.entrySet()) {
					RemoteHost remoteHost = entry.getKey();
					if (remoteHost.canDoSingleOp()) {
						unis.add(webClient.post(remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT)
								.putHeaders(remoteHost.headers()).sendJsonObject(new JsonObject(entry.getValue()))
								.onItemOrFailure().transformToUni((response, failure) -> {
									handleWebResponse(localResult, response, failure, 201, remoteHost,
											HttpUtils.getAttribsFromCompactedPayload(entry.getValue()));
									return Uni.createFrom().voidItem();
								}));
					} else {
						unis.add(webClient.post(remoteHost.host() + NGSIConstants.ENDPOINT_BATCH_CREATE)
								.putHeaders(remoteHost.headers())
								.sendJson(new JsonArray(Lists.newArrayList(new JsonObject(entry.getValue()))))
								.onItemOrFailure().transformToUni((response, failure) -> {
									NGSILDOperationResult remoteResult = handleBatchResponse(response, failure,
											remoteHost, Lists.newArrayList(entry.getValue()), ArrayUtils.toArray(201))
											.get(0);
									localResult.getSuccesses().addAll(remoteResult.getSuccesses());
									localResult.getFailures().addAll(remoteResult.getFailures());
									return Uni.createFrom().voidItem();
								}));
					}
				}
				if (unis.isEmpty()) {
					return Uni.createFrom().item(localResult);
				}
				return Uni.combine().all().unis(unis).combinedWith(t -> localResult);
			});

		});
	}

	public Uni<List<NGSILDOperationResult>> createMultipleEntry(String tenant,
			List<Tuple2<Context, Map<String, Object>>> entity2Context) {
		logger.debug("createMessage() :: started");
		BatchInfo batchInfo = new BatchInfo(random.nextInt(), entity2Context.size());

		List<Uni<Tuple2<NGSILDOperationResult, Map<RemoteHost, Map<String, Object>>>>> dbUnis = Lists.newArrayList();
		for (Tuple2<Context, Map<String, Object>> tuple : entity2Context) {
			CreateEntityRequest request = new CreateEntityRequest(tenant, tuple.getItem2(), batchInfo);
			dbUnis.add(entityDAO.createEntity(request).onItem().transformToUni(resultTable -> {
				if (resultTable.size() == 0) {
					return Uni.createFrom().failure(new ResponseException(ErrorType.InternalError,
							"No result from the database this should never happen"));
				}
				return handleDBCreateResult(request, resultTable, tuple.getItem1());
			}));
		}

		return Uni.combine().all().unis(dbUnis).combinedWith(list -> {

			ArrayListMultimap<RemoteHost, Map<String, Object>> remoteHost2Entities = ArrayListMultimap.create();
			Map<String, NGSILDOperationResult> entityId2Result = Maps.newHashMap();

			for (Object obj : list) {
				Tuple2<NGSILDOperationResult, Map<RemoteHost, Map<String, Object>>> tmp = (Tuple2<NGSILDOperationResult, Map<RemoteHost, Map<String, Object>>>) obj;
				NGSILDOperationResult result = tmp.getItem1();
				Map<RemoteHost, Map<String, Object>> remoteHost2Entity = tmp.getItem2();
				for (RemoteHost key : remoteHost2Entity.keySet()) {
					remoteHost2Entities.put(key, remoteHost2Entity.get(key));
				}
				entityId2Result.put(tmp.getItem1().getEntityId(), tmp.getItem1());
			}
			// collect all single entities for remote batch operations
			return Tuple2.of(remoteHost2Entities, entityId2Result);
		}).onItem().transformToUni(tuple -> {
			ArrayListMultimap<RemoteHost, Map<String, Object>> remoteHost2Entities = tuple.getItem1();

			List<Uni<List<NGSILDOperationResult>>> unis = new ArrayList<>(remoteHost2Entities.size());
			for (RemoteHost remoteHost : remoteHost2Entities.keys()) {
				List<Map<String, Object>> remoteEntities = remoteHost2Entities.get(remoteHost);
				if (remoteHost.canDoBatchOp()) {
					unis.add(webClient.post(remoteHost.host() + NGSIConstants.ENDPOINT_BATCH_CREATE)
							.putHeaders(remoteHost.headers()).sendJson(new JsonArray(remoteEntities)).onItemOrFailure()
							.transform((response, failure) -> {
								return handleBatchResponse(response, failure, remoteHost, remoteEntities,
										ArrayUtils.toArray(201));
							}));
				} else {
					// backup in case someone can't do batch op
					List<Uni<Tuple4<HttpResponse, Throwable, RemoteHost, Map<String, Object>>>> unisForHost = new ArrayList<>(
							remoteHost2Entities.size());
					for (Map<String, Object> remoteEntity : remoteEntities) {
						unisForHost.add(webClient.post(remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT)
								.putHeaders(remoteHost.headers()).sendJsonObject(new JsonObject(remoteEntity))
								.onItemOrFailure().transform((response, failure) -> {
									return Tuple4.of(response, failure, remoteHost, remoteEntity);
								}));
					}
					unis.add(Uni.combine().all().unis(unisForHost).combinedWith(hostList -> {
						Iterator<Tuple4<HttpResponse, Throwable, RemoteHost, Map<String, Object>>> it = (Iterator<Tuple4<HttpResponse, Throwable, RemoteHost, Map<String, Object>>>) hostList
								.iterator();
						Tuple4<HttpResponse, Throwable, RemoteHost, Map<String, Object>> next;
						List<Map<String, Object>> postedEntityList = new ArrayList<>(hostList.size());
						List<NGSILDOperationResult> opResults = new ArrayList<>(hostList.size());
						NGSILDOperationResult tmpResult;
						while (it.hasNext()) {
							next = it.next();
							tmpResult = new NGSILDOperationResult(AppConstants.CREATE_REQUEST,
									(String) next.getItem4().get("id"));
							postedEntityList.add(next.getItem4());
							handleWebResponse(tmpResult, next.getItem1(), next.getItem2(), 201, remoteHost,
									HttpUtils.getAttribsFromCompactedPayload(next.getItem4()));
							opResults.add(tmpResult);
						}
						return opResults;
					}));
				}
			}
			return Uni.combine().all().unis(unis).combinedWith(t -> {
				Map<String, NGSILDOperationResult> entityId2Result = tuple.getItem2();
				for (Object obj : t) {
					mergeRemoteResults(entityId2Result, (List<NGSILDOperationResult>) obj);
				}
				return Lists.newArrayList(entityId2Result.values());
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

	public Uni<NGSILDOperationResult> upsertEntry(String tenant, Map<String, Object> resolved,
			Context originalContext) {
		logger.debug("createMessage() :: started");
		CreateEntityRequest request = new CreateEntityRequest(tenant, resolved, null);
		return entityDAO.upsertEntity(request).onItem().transformToUni(resultTable -> {
			if (resultTable.size() == 0) {
				return Uni.createFrom().failure(new ResponseException(ErrorType.InternalError,
						"No result from the database this should never happen"));
			}
			return handleDBCreateResult(request, resultTable, originalContext).onItem().transform(tuple -> {
				NGSILDOperationResult localResult = tuple.getItem1();
				Map<RemoteHost, Map<String, Object>> remoteHosts = tuple.getItem2();
				List<Uni> unis = new ArrayList<>(remoteHosts.size());
				for (Entry<RemoteHost, Map<String, Object>> entry : remoteHosts.entrySet()) {
					RemoteHost remoteHost = entry.getKey();
					if (remoteHost.canDoSingleOp()) {
						unis.add(webClient.post(remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT)
								.putHeaders(remoteHost.headers()).sendJsonObject(new JsonObject(entry.getValue()))
								.onItemOrFailure().transformToUni((response, failure) -> {
									handleWebResponse(localResult, response, failure, 201, remoteHost,
											HttpUtils.getAttribsFromCompactedPayload(entry.getValue()));
									return Uni.createFrom().voidItem();
								}));
					}
				}

				return localResult;

			});
		});
	}

	/**
	 * @param request         the reate request
	 * @param resultTable     the RowSet coming from the DB
	 * @param originalContext the context used in the original request
	 * @return a Tuple of the local db result and remote hosts (tenant host headers)
	 */
	private Uni<Tuple2<NGSILDOperationResult, Map<RemoteHost, Map<String, Object>>>> handleDBCreateResult(
			CreateEntityRequest request, RowSet<Row> resultTable, Context context) {
		NGSILDOperationResult operationResult = new NGSILDOperationResult(AppConstants.CREATE_REQUEST, request.getId());
		Map<RemoteHost, Map<String, Object>> remoteResults = Maps.newHashMap();
		List<Uni<Void>> unis = Lists.newArrayList();
		Map<Integer, Map<String, Object>> hash2Compacted = Maps.newHashMap();

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
					operationResult.addFailure(new ResponseException(ErrorType.InternalError, row.getString(4)));
				}
				if (request.getBatchInfo() != null) {
					sendKafka = sendFail(request.getBatchInfo());
				}
				break;
			}
			case "ADDED ENTITY": {
				Map<String, Object> entityAdded = ((JsonObject) row.getJson(3)).getMap();
				request.setPayload(entityAdded);
				operationResult.addSuccess(new CRUDSuccess(null, null, null, entityAdded, context));
				sendKafka = kafkaSenderInterface.send(request);
				break;
			}
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
						compacted = JsonLdProcessor.compact(entityToForward, null, context, opts, -1);
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
			if (request.getBatchInfo() != null) {
				sendKafka = sendFail(request.getBatchInfo());
			} else {
				sendKafka = Uni.createFrom().voidItem();
			}
		}
		return sendKafka.onItem().transform(v -> Tuple2.of(operationResult, remoteResults));
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

	/**
	 * Method to update a existing Entity in the system/kafka topic
	 *
	 * @param entityId - id of entity to be updated
	 * @return RestResponse
	 * @throws ResponseException
	 * @throws IOException
	 */
	public Uni<NGSILDOperationResult> updateEntry(String tenant, String entityId, Map<String, Object> payload,
			Context originalContext, BatchInfo batchInfo) {
		logger.trace("updateMessage() :: started");
		UpdateEntityRequest request = new UpdateEntityRequest(tenant, entityId, payload, null, batchInfo);
		return entityDAO.updateEntity(request).onItem().transformToUni(resultTable -> {
			if (resultTable.size() == 0) {
				return Uni.createFrom().failure(new ResponseException(ErrorType.InternalError,
						"No result from the database this should never happen"));
			}
			return handleDBUpdateResult(request, resultTable, originalContext).onItem().transformToUni(tuple -> {
				NGSILDOperationResult localResult = tuple.getItem1();
				Map<RemoteHost, Map<String, Object>> remoteHosts = tuple.getItem2();
				List<Uni<Void>> unis = new ArrayList<>(remoteHosts.size());
				for (Entry<RemoteHost, Map<String, Object>> entry : remoteHosts.entrySet()) {
					RemoteHost remoteHost = entry.getKey();
					if (remoteHost.canDoSingleOp()) {
						if (request.getAttrName() != null) {
							unis.add(webClient
									.patch(remoteHost + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "/" + request.getId()
											+ "/attrs/" + request.getAttrName())
									.putHeaders(remoteHost.headers()).sendJsonObject(new JsonObject(entry.getValue()))
									.onItemOrFailure().transformToUni((response, failure) -> {
										handleWebResponse(localResult, response, failure, 201, remoteHost,
												HttpUtils.getAttribsFromCompactedPayload(entry.getValue()));
										return Uni.createFrom().voidItem();
									}));
						} else {
							unis.add(webClient
									.patch(remoteHost + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "/" + request.getId()
											+ "/attrs/")
									.putHeaders(remoteHost.headers()).sendJsonObject(new JsonObject(entry.getValue()))
									.onItemOrFailure().transformToUni((response, failure) -> {
										handleWebResponse(localResult, response, failure, 201, remoteHost,
												HttpUtils.getAttribsFromCompactedPayload(entry.getValue()));
										return Uni.createFrom().voidItem();
									}));
						}

					} else {
						unis.add(webClient.post(remoteHost.host() + NGSIConstants.ENDPOINT_BATCH_UPDATE)
								.putHeaders(remoteHost.headers())
								.sendJson(new JsonArray(Lists.newArrayList(new JsonObject(entry.getValue()))))
								.onItemOrFailure().transformToUni((response, failure) -> {
									NGSILDOperationResult remoteResult = handleBatchResponse(response, failure,
											remoteHost, Lists.newArrayList(entry.getValue()), ArrayUtils.toArray(201))
											.get(0);
									localResult.getSuccesses().addAll(remoteResult.getSuccesses());
									localResult.getFailures().addAll(remoteResult.getFailures());
									return Uni.createFrom().voidItem();
								}));
					}
				}
				return Uni.combine().all().unis(unis).combinedWith(t -> localResult);
			});
		});
	}

	private Uni<Tuple2<NGSILDOperationResult, Map<RemoteHost, Map<String, Object>>>> handleDBUpdateResult(
			EntityRequest request, RowSet<Row> resultTable, Context originalContext) {
		NGSILDOperationResult result = new NGSILDOperationResult(AppConstants.UPDATE_REQUEST, request.getId());
		List<Uni<Void>> unis = Lists.newArrayList();
		Map<RemoteHost, Map<String, Object>> remoteResults = Maps.newHashMap();
		Map<Integer, Map<String, Object>> hash2Compacted = Maps.newHashMap();
		boolean internalSend = false;
		Context context = JsonLdProcessor.getCoreContextClone().parse(originalContext, true);
		RowIterator<Row> it = resultTable.iterator();
		while (it.hasNext()) {
			Row row = it.next();

			switch (row.getString(0)) {
			case "ERROR": {
				// JsonObject error = ((JsonObject) row.getJson(3));
				// for now the only "error" in sql would be if someone tries to remove a type
				// if this changes in sql we need to adapt here
				result.addFailure(
						new ResponseException(ErrorType.BadRequestData, "You cannot remove a type from an entity"));
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
				result.addSuccess(new CRUDSuccess(null, null, null, attribs));
				internalSend = true;
				unis.add(kafkaSenderInterface.send(request));
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
				result.addFailure(new ResponseException(ErrorType.NotFound.getCode(), ErrorType.NotFound.getType(),
						ErrorType.NotFound.getTitle(), ErrorType.NotFound.getTitle(), null, null, null, attribs));
			}
			default:
				String host = row.getString(0);
				String tenant = row.getString(1);
				Map<String, Object> entityToForward = ((JsonObject) row.getJson(3)).getMap();
				MultiMap headers = MultiMap
						.newInstance(HttpUtils.getHeadersForRemoteCall((JsonArray) row.getJson(2), tenant));
				String cSourceId = ""; // not in the result for now row.getString(4);
				int hash = entityToForward.hashCode();
				Map<String, Object> compacted;
				if (!hash2Compacted.containsKey(hash)) {
					try {
						compacted = JsonLdProcessor.compact(entityToForward, originalContext, opts);
					} catch (ResponseException e) {
						// TODO add host info
						result.addFailure(e);
						break;
					} catch (Exception e) {
						// TODO add host info
						result.addFailure(new ResponseException(ErrorType.InternalError, e.getMessage()));
						break;
					}
					hash2Compacted.put(hash, compacted);
				} else {
					compacted = hash2Compacted.get(hash);
				}
				/*
				 * String url; HttpRequest<Buffer> req; if (request.getRequestType() ==
				 * AppConstants.UPDATE_REQUEST) { if (((UpdateEntityRequest)
				 * request).getAttrName() != null) { req = webClient.patch(host +
				 * NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "/" + request.getId() + "/attrs/" +
				 * ((UpdateEntityRequest) request).getAttrName()); } else { req =
				 * webClient.patch( host + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "/" +
				 * request.getId() + "/attrs"); } } else { req = webClient .post(host +
				 * NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "/" + request.getId() + "/attrs");
				 * }
				 * 
				 * unis.add(req.putHeaders(headers).sendJsonObject(new
				 * JsonObject(compacted)).onItemOrFailure() .transformToUni((response, failure)
				 * -> { // handleWebResponse(result, response, failure, 201, host, headers,
				 * cSourceId, // entityToForward, // context); handleWebResponse(result,
				 * response, failure, 201, host,
				 * HttpUtils.getAttribsFromCompactedPayload(compacted));
				 */

				remoteResults.put(new RemoteHost(host, tenant, null, cSourceId, row.getBoolean(5), row.getBoolean(6)),
						compacted);
//							return Uni.createFrom().voidItem();
//						}));
				break;
			}

		}
		if (!internalSend) {
			unis.add(sendFail(request.getBatchInfo()));
		}
//		return Uni.combine().all().unis(unis).combinedWith(remoteEntries -> result);
		return Uni.createFrom().item(Tuple2.of(result, remoteResults));
	}

	public Uni<Void> sendFail(BatchInfo batchInfo) {
		BaseRequest request = new BaseRequest();
		request.setRequestType(AppConstants.BATCH_ERROR_REQUEST);
		request.setBatchInfo(batchInfo);
		request.setId("" + batchInfo.getBatchId());
		return kafkaSenderInterface.send(request);
	}

	public Uni<NGSILDOperationResult> updateEntry(String tenant, String entityId, Map<String, Object> entry,
			Context originalContext) {
		return updateEntry(tenant, entityId, entry, originalContext, new BatchInfo(-1, -1));
	}

	public Uni<NGSILDOperationResult> appendToEntry(String tenant, String entityId, Map<String, Object> entry,
			boolean noOverwrite, Context originalContext) {
		AppendEntityRequest request = new AppendEntityRequest(tenant, entityId, entry, null);
		return entityDAO.appendEntity(request, noOverwrite).onItem().transformToUni(resultTable -> {
			if (resultTable.size() == 0) {
				return Uni.createFrom().failure(new ResponseException(ErrorType.InternalError,
						"No result from the database this should never happen"));
			}
			return handleDBUpdateResult(request, resultTable, originalContext).onItem().transformToUni(tuple -> {
				NGSILDOperationResult localResult = tuple.getItem1();
				Map<RemoteHost, Map<String, Object>> remoteHosts = tuple.getItem2();
				List<Uni<Void>> unis = new ArrayList<>(remoteHosts.size());
				for (Entry<RemoteHost, Map<String, Object>> entry1 : remoteHosts.entrySet()) {
					RemoteHost remoteHost = entry1.getKey();
					if (remoteHost.canDoSingleOp()) {
						unis.add(webClient
								.post(remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "/" + entityId
										+ "/attrs")
								.putHeaders(remoteHost.headers()).sendJsonObject(new JsonObject(entry1.getValue()))
								.onItemOrFailure().transformToUni((response, failure) -> {
									handleWebResponse(localResult, response, failure, 201, remoteHost,
											HttpUtils.getAttribsFromCompactedPayload(entry1.getValue()));
									return Uni.createFrom().voidItem();
								}));
					} else {
						unis.add(webClient.post(remoteHost.host() + NGSIConstants.ENDPOINT_BATCH_UPDATE)
								.putHeaders(remoteHost.headers())
								.sendJson(new JsonArray(Lists.newArrayList(new JsonObject(entry1.getValue()))))
								.onItemOrFailure().transformToUni((response, failure) -> {
									NGSILDOperationResult remoteResult = handleBatchResponse(response, failure,
											remoteHost, Lists.newArrayList(entry1.getValue()), ArrayUtils.toArray(201))
											.get(0);
									localResult.getSuccesses().addAll(remoteResult.getSuccesses());
									localResult.getFailures().addAll(remoteResult.getFailures());
									return Uni.createFrom().voidItem();
								}));
					}
				}
				return Uni.combine().all().unis(unis).combinedWith(t -> localResult);
			});
		});
	}

	public Uni<NGSILDOperationResult> deleteEntry(String tenant, String entityId, Context context) {
		DeleteEntityRequest request = new DeleteEntityRequest(tenant, entityId, null);
		return entityDAO.deleteEntity(request).onItem().transformToUni(resultTable -> {
			if (resultTable.size() == 0) {
				return Uni.createFrom().failure(new ResponseException(ErrorType.InternalError,
						"No result from the database this should never happen"));
			}
			NGSILDOperationResult result = new NGSILDOperationResult(AppConstants.UPDATE_REQUEST, request.getId());
			List<Uni<Void>> unis = Lists.newArrayList();
			Map<Integer, Map<String, Object>> hash2Compacted = Maps.newHashMap();

			resultTable.forEach(row -> {

				switch (row.getString(0)) {
				case "DELETED ENTITY": {
					request.setPayload(row.getJsonObject(3).getMap());
					result.addSuccess(new CRUDSuccess(null, null, null, request.getPayload(), context));
					unis.add(kafkaSenderInterface.send(request));
					break;
				}
				case "ERROR": {
					// [{"attribName": attribName, "datasetId": datasetId}]
					String errorCode = row.getString(1);
					if (errorCode.equals(AppConstants.SQL_NOT_FOUND)) {
						result.addFailure(new ResponseException(ErrorType.NotFound, ErrorType.NotFound.getTitle(),
								Sets.newHashSet()));
					} else {
						result.addFailure(
								new ResponseException(ErrorType.InternalError, row.getString(2), Sets.newHashSet()));
					}
				}
				default:
					String host = row.getString(0);
					String tenantHost = row.getString(1);
					// This is not a final solution ... but in the case of full success on the other
					// side there is no way of know what has been deleted so ... don't know how to
					// handle that
					Map<String, Object> entityToForward = Maps.newHashMap();
					MultiMap remoteHeaders = MultiMap
							.newInstance(HttpUtils.getHeadersForRemoteCall((JsonArray) row.getJson(2), tenant));
					String cSourceId = row.getString(4); // not in the result for now row.getString(4);
					Set<Attrib> attribs = new HashSet<>();
					attribs.add(new Attrib(null, entityId));
					RemoteHost remoteHost = new RemoteHost(host, tenant, remoteHeaders, cSourceId, row.getBoolean(5),
							row.getBoolean(6));

					unis.add(webClient.delete(host + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "/" + request.getId())
							.putHeaders(remoteHeaders).send().onItemOrFailure().transformToUni((response, failure) -> {
								return handleWebResponse(result, response, failure, 204, remoteHost, attribs);
							}));
					break;
				}

			});

			return Uni.combine().all().unis(unis).combinedWith(remoteEntries -> result);

		});
	}

	public Uni<NGSILDOperationResult> partialUpdateEntity(String tenant, String entityId, String attribName,
			Map<String, Object> payload, Context originalContext) {
		logger.trace("updateMessage() :: started");
		// place the id in the payload to be sure

		Map<String, Object> effectivePayload;
		if (payload.containsKey(attribName)) {
			effectivePayload = payload;
		} else {
			effectivePayload = Maps.newHashMap();
			effectivePayload.put(attribName, payload);
		}
		effectivePayload.put(NGSIConstants.JSON_LD_ID, entityId);
		UpdateEntityRequest request = new UpdateEntityRequest(tenant, entityId, effectivePayload, attribName,
				new BatchInfo(-1, -1));
		return entityDAO.partialUpdateEntity(request).onItem().transformToUni(resultTable -> {
			if (resultTable.size() == 0) {
				return Uni.createFrom().failure(new ResponseException(ErrorType.InternalError,
						"No result from the database this should never happen"));
			}
			return handleDBUpdateResult(request, resultTable, originalContext).onItem().transformToUni(tuple -> {
				NGSILDOperationResult localResult = tuple.getItem1();
				Map<RemoteHost, Map<String, Object>> remoteHosts = tuple.getItem2();
				List<Uni<Void>> unis = new ArrayList<>(remoteHosts.size());
				for (Entry<RemoteHost, Map<String, Object>> entry : remoteHosts.entrySet()) {
					RemoteHost remoteHost = entry.getKey();
					if (remoteHost.canDoSingleOp()) {
						unis.add(webClient
								.patch(remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "/" + entityId
										+ "/attrs/" + attribName)
								.putHeaders(remoteHost.headers()).sendJsonObject(new JsonObject(entry.getValue()))
								.onItemOrFailure().transformToUni((response, failure) -> {
									handleWebResponse(localResult, response, failure, 201, remoteHost,
											HttpUtils.getAttribsFromCompactedPayload(entry.getValue()));
									return Uni.createFrom().voidItem();
								}));
					} else {
						unis.add(webClient.post(remoteHost.host() + NGSIConstants.ENDPOINT_BATCH_UPDATE)
								.putHeaders(remoteHost.headers())
								.sendJson(new JsonArray(Lists.newArrayList(new JsonObject(entry.getValue()))))
								.onItemOrFailure().transformToUni((response, failure) -> {
									NGSILDOperationResult remoteResult = handleBatchResponse(response, failure,
											remoteHost, Lists.newArrayList(entry.getValue()), ArrayUtils.toArray(201))
											.get(0);
									localResult.getSuccesses().addAll(remoteResult.getSuccesses());
									localResult.getFailures().addAll(remoteResult.getFailures());
									return Uni.createFrom().voidItem();
								}));
					}
				}
				return Uni.combine().all().unis(unis).combinedWith(t -> localResult);
			});
		});
	}

	public Uni<NGSILDOperationResult> deleteAttribute(String tenant, String entityId, String attribName,
			String datasetId, boolean deleteAll, Context originalContext) {
		logger.trace("updateMessage() :: started");
		// place the id in the payload to be sure

		DeleteAttributeRequest request = new DeleteAttributeRequest(tenant, entityId, attribName, datasetId, deleteAll);
		return entityDAO.deleteAttribute(request).onItem().transformToUni(resultTable -> {
			if (resultTable.size() == 0) {
				return Uni.createFrom().failure(new ResponseException(ErrorType.InternalError,
						"No result from the database this should never happen"));
			}
			NGSILDOperationResult result = new NGSILDOperationResult(AppConstants.UPDATE_REQUEST, request.getId());
			List<Uni<Void>> unis = Lists.newArrayList();
			Map<Integer, Map<String, Object>> hash2Compacted = Maps.newHashMap();
			Context context = JsonLdProcessor.getCoreContextClone().parse(originalContext, true);
			resultTable.forEach(row -> {

				switch (row.getString(0)) {
				case "DELETED ATTRIB": {
					result.addSuccess(
							new CRUDSuccess(null, null, null, Sets.newHashSet(new Attrib(attribName, datasetId))));
					break;
				}
				case "RESULT ENTITY": {
					// [{"attribName": attribName, "datasetId": datasetId}]
					Map<String, Object> entity = row.getJsonObject(3).getMap();
					request.setPayload(entity);
					unis.add(kafkaSenderInterface.send(request));
					break;
				}
				case "ERROR": {
					// [{"attribName": attribName, "datasetId": datasetId}]
					String errorCode = row.getString(1);
					if (errorCode.equals(AppConstants.SQL_NOT_FOUND)) {
						result.addFailure(new ResponseException(ErrorType.NotFound, ErrorType.NotFound.getTitle(),
								Sets.newHashSet(new Attrib(attribName, datasetId))));
					} else {
						result.addFailure(new ResponseException(ErrorType.InternalError, row.getString(2),
								Sets.newHashSet(new Attrib(attribName, datasetId))));
					}
				}
				default:
					String host = row.getString(0);
					String forwardTenant = row.getString(1);
					Map<String, Object> entityToForward = Maps.newHashMap();
					Map<String, Object> dummy = Maps.newHashMap();
					entityToForward.put(attribName, Lists.newArrayList(dummy));
					MultiMap remoteHeaders = MultiMap
							.newInstance(HttpUtils.getHeadersForRemoteCall((JsonArray) row.getJson(2), tenant));
					String cSourceId = row.getString(4); // not in the result for now row.getString(4);

					StringBuilder url = new StringBuilder(host + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "/"
							+ request.getId() + "/attrs/" + attribName);
					if (datasetId != null) {

						dummy.put("datasetId", datasetId);
						url.append("?datasetId=");
						url.append(datasetId);
						if (deleteAll) {
							url.append("&deleteAll");
						}
					} else {
						if (deleteAll) {
							url.append("?deleteAll");
						}
					}
					Set<Attrib> attribs = new HashSet<>();
					attribs.add(new Attrib(attribName, datasetId));
					RemoteHost remoteHost = new RemoteHost(host, tenant, remoteHeaders, cSourceId, row.getBoolean(5),
							row.getBoolean(6));
					unis.add(webClient.delete(url.toString()).putHeaders(remoteHeaders).send().onItemOrFailure()
							.transformToUni((response, failure) -> handleWebResponse(result, response, failure, 204,
									remoteHost, attribs)));
					break;
				}

			});

			return Uni.combine().all().unis(unis).combinedWith(remoteEntries -> result);

		});
	}

	public Uni<List<NGSILDOperationResult>> updateMultipleEntry(String tenant,
			List<Tuple3<Context, Map<String, Object>, Boolean>> entity2Context) {
		BatchInfo batchInfo = new BatchInfo(random.nextInt(), entity2Context.size());
		List<Uni<Tuple2<NGSILDOperationResult, Map<RemoteHost, Map<String, Object>>>>> dbUnis = Lists.newArrayList();
		for (Tuple3<Context, Map<String, Object>, Boolean> tuple : entity2Context) {
			AppendEntityRequest request = new AppendEntityRequest(tenant,
					(String) tuple.getItem2().get(NGSIConstants.JSON_LD_ID), tuple.getItem2(), batchInfo);
			dbUnis.add(entityDAO.appendEntity(request, tuple.getItem3()).onItem().transformToUni(resultTable -> {
				if (resultTable.size() == 0) {
					return Uni.createFrom().failure(new ResponseException(ErrorType.InternalError,
							"No result from the database this should never happen"));
				}
				return handleDBUpdateResult(request, resultTable, tuple.getItem1());

			}));
		}
		return Uni.combine().all().unis(dbUnis).combinedWith(list -> {

			ArrayListMultimap<RemoteHost, Map<String, Object>> remoteHost2Entities = ArrayListMultimap.create();
			Map<String, NGSILDOperationResult> entityId2Result = Maps.newHashMap();

			for (Object obj : list) {
				Tuple2<NGSILDOperationResult, Map<RemoteHost, Map<String, Object>>> tmp = (Tuple2<NGSILDOperationResult, Map<RemoteHost, Map<String, Object>>>) obj;
				NGSILDOperationResult result = tmp.getItem1();
				Map<RemoteHost, Map<String, Object>> remoteHost2Entity = tmp.getItem2();
				for (RemoteHost key : remoteHost2Entity.keySet()) {
					remoteHost2Entities.put(key, remoteHost2Entity.get(key));
				}
				entityId2Result.put(result.getEntityId(), result);
			}
			// collect all single entities for remote batch operations
			return Tuple2.of(remoteHost2Entities, entityId2Result);
		}).onItem().transformToUni(tupleItem -> {
			ArrayListMultimap<RemoteHost, Map<String, Object>> remoteHost2Entities = tupleItem.getItem1();

			List<Uni<List<NGSILDOperationResult>>> unis = new ArrayList<>(remoteHost2Entities.size());
			for (RemoteHost remoteHost : remoteHost2Entities.keys()) {
				List<Map<String, Object>> remoteEntities = remoteHost2Entities.get(remoteHost);
				if (remoteHost.canDoBatchOp()) {
					unis.add(webClient.post(remoteHost.host() + NGSIConstants.ENDPOINT_BATCH_UPDATE)
							.putHeaders(remoteHost.headers()).sendJson(new JsonArray(remoteEntities)).onItemOrFailure()
							.transform((response, failure) -> handleBatchResponse(response, failure, remoteHost,
									remoteEntities, ArrayUtils.toArray(201))));
				} else {
					// backup in case someone can't do batch op
					List<Uni<Tuple4<HttpResponse, Throwable, RemoteHost, Map<String, Object>>>> unisForHost = new ArrayList<>(
							remoteHost2Entities.size());
					for (Map<String, Object> remoteEntity : remoteEntities) {
						unisForHost.add(webClient
								.patch(remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "/"
										+ remoteEntity.get("id") + "/attrs")
								.putHeaders(remoteHost.headers()).sendJsonObject(new JsonObject(remoteEntity))
								.onItemOrFailure().transform((response, failure) -> {
									return Tuple4.of(response, failure, remoteHost, remoteEntity);
								}));
					}
					unis.add(Uni.combine().all().unis(unisForHost).combinedWith(hostList -> {
						Iterator<Tuple4<HttpResponse, Throwable, RemoteHost, Map<String, Object>>> it = (Iterator<Tuple4<HttpResponse, Throwable, RemoteHost, Map<String, Object>>>) hostList
								.iterator();
						Tuple4<HttpResponse, Throwable, RemoteHost, Map<String, Object>> next;
						List<Map<String, Object>> postedEntityList = new ArrayList<>(hostList.size());
						List<NGSILDOperationResult> opResults = new ArrayList<>(hostList.size());
						NGSILDOperationResult tmpResult;
						while (it.hasNext()) {
							next = it.next();
							tmpResult = new NGSILDOperationResult(AppConstants.UPDATE_REQUEST,
									(String) next.getItem4().get("id"));
							postedEntityList.add(next.getItem4());
							handleWebResponse(tmpResult, next.getItem1(), next.getItem2(), 201, remoteHost,
									HttpUtils.getAttribsFromCompactedPayload(next.getItem4()));
							opResults.add(tmpResult);
						}
						return opResults;
					}));
				}
			}
			return Uni.combine().all().unis(unis).combinedWith(t -> {
				Map<String, NGSILDOperationResult> entityId2Result = tupleItem.getItem2();
				for (Object obj : t) {
					mergeRemoteResults(entityId2Result, (List<NGSILDOperationResult>) obj);
				}
				return Lists.newArrayList(entityId2Result.values());
			});

		});
	}

	public Uni<List<NGSILDOperationResult>> deleteMultipleEntry(String tenant, List<String> idsAndContext,
			BatchInfo batchInfo) {

//        BatchInfo batchInfo = new BatchInfo(random.nextInt(), idsAndContext.size());

		List<Uni<Tuple2<NGSILDOperationResult, Map<RemoteHost, String>>>> dbUnis = Lists.newArrayList();
		for (String item : idsAndContext) {
			DeleteEntityRequest request = new DeleteEntityRequest(tenant, item, batchInfo);
			dbUnis.add(entityDAO.deleteEntity(request).onItem().transformToUni(resultTable -> {
				if (resultTable.size() == 0) {
					return Uni.createFrom().failure(new ResponseException(ErrorType.InternalError,
							"No result from the database this should never happen"));
				}
				NGSILDOperationResult result = new NGSILDOperationResult(AppConstants.UPDATE_REQUEST, request.getId());
				Map<RemoteHost, String> remoteResults = new HashMap<>();
//                List<Uni<Void>> unis = Lists.newArrayList();
				Map<Integer, Map<String, Object>> hash2Compacted = Maps.newHashMap();
//                Context context = JsonLdProcessor.getCoreContextClone().parse(originalContext, true);
				resultTable.forEach(row -> {

					switch (row.getString(0)) {
					case "DELETED ENTITY": {
						request.setPayload(row.getJsonObject(3).getMap());
						result.addSuccess(new CRUDSuccess(null, null, null, null, null));
//                            unis.add(kafkaSenderInterface.send(request));
						break;
					}
					case "ERROR": {
						// [{"attribName": attribName, "datasetId": datasetId}]
						String errorCode = row.getString(1);
						if (errorCode.equals(AppConstants.SQL_NOT_FOUND)) {
							result.addFailure(new ResponseException(ErrorType.NotFound, ErrorType.NotFound.getTitle(),
									Sets.newHashSet()));
						} else {
							result.addFailure(new ResponseException(ErrorType.InternalError, row.getString(2),
									Sets.newHashSet()));
						}
					}
					default:
						String host = row.getString(0);
						String tenantHost = row.getString(1);
						// This is not a final solution ... but in the case of full success on the other
						// side there is no way of know what has been deleted so ... don't know how to
						// handle that
//                            Map<String, Object> entityToForward = Maps.newHashMap();
						MultiMap remoteHeaders = MultiMap
								.newInstance(HttpUtils.getHeadersForRemoteCall((JsonArray) row.getJson(2), tenant));
						String cSourceId = row.getString(4); // not in the result for now row.getString(4);
//                            Set<Attrib> attribs = new HashSet<>();
//                            attribs.add(new Attrib(null, entityId));
						RemoteHost remoteHost = new RemoteHost(host, tenant, remoteHeaders, cSourceId,
								row.getBoolean(5), row.getBoolean(6));

//                            unis.add(webClient.delete(host + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "/" + request.getId()).putHeaders(remoteHeaders).send().onItemOrFailure()
//                                    .transformToUni((response, failure) -> {
//                                        return handleWebResponse(result, response, failure, 204, remoteHost, attribs);
//                                    }));
						remoteResults.put(
								new RemoteHost(host, tenant, null, cSourceId, row.getBoolean(5), row.getBoolean(6)),
								request.getId());
						break;
					}

				});

				return Uni.createFrom().item(Tuple2.of(result, remoteResults));

			}));
		}
		return Uni.combine().all().unis(dbUnis).combinedWith(list -> {

			ArrayListMultimap<RemoteHost, String> remoteHost2Ids = ArrayListMultimap.create();
			Map<String, NGSILDOperationResult> entityId2Result = Maps.newHashMap();

			for (Object obj : list) {
				Tuple2<NGSILDOperationResult, Map<RemoteHost, String>> tmp = (Tuple2<NGSILDOperationResult, Map<RemoteHost, String>>) obj;
				NGSILDOperationResult result = tmp.getItem1();
				Map<RemoteHost, String> remoteHost2Id = tmp.getItem2();
//                        for (Entry<RemoteHost, Map<String, Object>> entry : remoteHost2Ids.entries()) {
//                            remoteHost2Entities.put(entry.getKey(), entry.getValue());
//                        }
				for (RemoteHost rh : remoteHost2Id.keySet()) {
					remoteHost2Ids.put(rh, remoteHost2Id.get(rh));
				}
				entityId2Result.put(tmp.getItem1().getEntityId(), tmp.getItem1());
			}
			// collect all single entities for remote batch operations
			return Tuple2.of(remoteHost2Ids, entityId2Result);
		}).onItem().transformToUni(tuple -> {
			ArrayListMultimap<RemoteHost, String> remoteHost2Ids = tuple.getItem1();

			List<Uni<List<NGSILDOperationResult>>> unis = new ArrayList<>(remoteHost2Ids.size());
			for (RemoteHost remoteHost : remoteHost2Ids.keys()) {
				List<String> remoteIds = remoteHost2Ids.get(remoteHost);
				if (remoteHost.canDoBatchOp()) {
					unis.add(webClient.post(remoteHost.host() + NGSIConstants.ENDPOINT_BATCH_DELETE)
							.putHeaders(remoteHost.headers()).sendJson(new JsonArray(remoteIds)).onItemOrFailure()
							.transform((response, failure) -> {
								return handleBatchResponse(response, failure, remoteHost, null,
										ArrayUtils.toArray(201));
							}));
				} else {
					// backup in case someone can't do batch op
					List<Uni<Tuple4<HttpResponse, Throwable, RemoteHost, String>>> unisForHost = new ArrayList<>(
							remoteHost2Ids.size());
					for (String remoteId : remoteIds) {
						unisForHost.add(webClient.delete(remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT)
								.putHeaders(remoteHost.headers()).sendJsonObject(new JsonObject(remoteId))
								.onItemOrFailure().transform((response, failure) -> {
									return Tuple4.of(response, failure, remoteHost, remoteId);
								}));
					}
					unis.add(Uni.combine().all().unis(unisForHost).combinedWith(hostList -> {
						Iterator<Tuple4<HttpResponse, Throwable, RemoteHost, Map<String, Object>>> it = (Iterator<Tuple4<HttpResponse, Throwable, RemoteHost, Map<String, Object>>>) hostList
								.iterator();
						Tuple4<HttpResponse, Throwable, RemoteHost, Map<String, Object>> next;
						List<Map<String, Object>> postedEntityList = new ArrayList<>(hostList.size());
						List<NGSILDOperationResult> opResults = new ArrayList<>(hostList.size());
						NGSILDOperationResult tmpResult;
						while (it.hasNext()) {
							next = it.next();
							tmpResult = new NGSILDOperationResult(AppConstants.CREATE_REQUEST,
									(String) next.getItem4().get("id"));
							postedEntityList.add(next.getItem4());
							handleWebResponse(tmpResult, next.getItem1(), next.getItem2(), 201, remoteHost,
									HttpUtils.getAttribsFromCompactedPayload(next.getItem4()));
							opResults.add(tmpResult);
						}
						return opResults;
					}));
				}
			}
			return Uni.combine().all().unis(unis).combinedWith(t -> {
				Map<String, NGSILDOperationResult> entityId2Result = tuple.getItem2();
				for (Object obj : t) {
					mergeRemoteResults(entityId2Result, (List<NGSILDOperationResult>) obj);
				}
				return Lists.newArrayList(entityId2Result.values());
			});

		});

	}

	public Uni<NGSILDOperationResult> updateEntry2(String tenant, String entityId, Map<String, Object> payload,
			Context originalContext) {
		UpdateEntityRequest request = new UpdateEntityRequest(tenant, entityId, payload, null, null);
		// entityDAO.updateEntity2(request)
		return null;
	}

	public Uni<NGSILDOperationResult> createEntry2(String tenant, Map<String, Object> resolved, Context context) {
		logger.debug("createMessage() :: started");
		System.out.println("before dao" + System.nanoTime());
		CreateEntityRequest request = new CreateEntityRequest(tenant, resolved, null);
		Tuple2<Map<String, Object>, Collection<Tuple2<RemoteHost, Map<String, Object>>>> localAndRemote = splitEntity(
				request);
		Map<String, Object> localEntity = localAndRemote.getItem1();
		Collection<Tuple2<RemoteHost, Map<String, Object>>> remoteEntitiesAndHosts = localAndRemote.getItem2();
		if (remoteEntitiesAndHosts.isEmpty()) {
			request.setPayload(localEntity);
			System.out.println("before dao " + System.nanoTime());
			return entityDAO.create2(request, context).onItem().transformToUni(v -> {
				System.out.println("after dao " + System.nanoTime());
				return kafkaSenderInterface.send(request).onItem().transform(v2 -> {
					NGSILDOperationResult localResult = new NGSILDOperationResult(AppConstants.CREATE_REQUEST,
							request.getId());
					localResult.addSuccess(new CRUDSuccess(null, null, null, request.getPayload(), context));
					return localResult;
				});
			});
		}
		List<Uni<NGSILDOperationResult>> unis = new ArrayList<>(remoteEntitiesAndHosts.size());
		for (Tuple2<RemoteHost, Map<String, Object>> remoteEntityAndHost : remoteEntitiesAndHosts) {
			Map<String, Object> expanded = remoteEntityAndHost.getItem2();
			expanded.put(NGSIConstants.JSON_LD_TYPE,
					Lists.newArrayList((Set<String>) expanded.get(NGSIConstants.JSON_LD_TYPE)));
			if (expanded.containsKey(NGSIConstants.NGSI_LD_SCOPE)) {
				Set<String> collectedScopes = (Set<String>) expanded.get(NGSIConstants.NGSI_LD_SCOPE);
				List<Map<String, String>> finalScopes = Lists.newArrayList();
				for (String scope : collectedScopes) {
					finalScopes.add(Map.of(NGSIConstants.JSON_LD_VALUE, scope));
				}
				expanded.put(NGSIConstants.NGSI_LD_SCOPE, finalScopes);
			}
			Map<String, Object> compacted;
			try {
				compacted = JsonLdProcessor.compact(expanded, null, context, opts, -1);
			} catch (JsonLdError | ResponseException e) {
				logger.error("Failed to compact remote payload", e);
				continue;
			}
			RemoteHost remoteHost = remoteEntityAndHost.getItem1();
			if (remoteHost.canDoSingleOp()) {
				unis.add(webClient.post(remoteHost.host() + NGSIConstants.NGSI_LD_TEMPORAL_ENTITIES_ENDPOINT)
						.putHeaders(remoteHost.headers()).sendJsonObject(new JsonObject(compacted)).onItemOrFailure()
						.transform((response, failure) -> {
							return HttpUtils.handleWebResponse(response, failure, ArrayUtils.toArray(201), remoteHost,
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

		}
		if (remoteEntitiesAndHosts.isEmpty()) {
			request.setPayload(localEntity);
			unis.add(entityDAO.create2(request, context).onItem().transformToUni(v -> {
				return kafkaSenderInterface.send(request).onItem().transform(v2 -> {
					NGSILDOperationResult localResult = new NGSILDOperationResult(AppConstants.CREATE_REQUEST,
							request.getId());
					localResult.addSuccess(new CRUDSuccess(null, null, null, request.getPayload(), context));
					return localResult;
				});
			}));
		}
		return Uni.combine().all().unis(unis).combinedWith(list -> {
			Iterator<?> it = list.iterator();
			NGSILDOperationResult operationResult = (NGSILDOperationResult) it.next();
			while (it.hasNext()) {
				NGSILDOperationResult tmp = (NGSILDOperationResult) it.next();
				operationResult.getSuccesses().addAll(tmp.getSuccesses());
				operationResult.getFailures().addAll(tmp.getFailures());
			}
			return operationResult;
		});
	}

	private Tuple2<Map<String, Object>, Collection<Tuple2<RemoteHost, Map<String, Object>>>> splitEntity(
			EntityRequest request) {
		Map<String, Object> originalEntity = request.getPayload();
		Collection<RegistrationEntry> regs = tenant2CId2RegEntries.column(request.getTenant()).values();
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
				if (!regEntry.createEntity() && !regEntry.createBatch()) {
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
						case AppConstants.CREATE_REQUEST:
							host = new RemoteHost(regHost.host(), regHost.tenant(), regHost.headers(),
									regHost.cSourceId(), regEntry.createEntity(), regEntry.createBatch());
							break;
						case AppConstants.APPEND_REQUEST:
							host = new RemoteHost(regHost.host(), regHost.tenant(), regHost.headers(),
									regHost.cSourceId(), regEntry.appendAttrs(), false);
							break;
						case AppConstants.UPDATE_REQUEST:
							host = new RemoteHost(regHost.host(), regHost.tenant(), regHost.headers(),
									regHost.cSourceId(), regEntry.updateAttrs(), regEntry.updateBatch());
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

	public Uni<Void> handleRegistryChange(CSourceRequest req) {
		tenant2CId2RegEntries.remove(req.getTenant(), req.getId());
		if (req.getRequestType() != AppConstants.DELETE_REQUEST) {
			for (RegistrationEntry regEntry : RegistrationEntry.fromRegPayload(req.getPayload())) {
				tenant2CId2RegEntries.put(req.getTenant(), req.getId(), regEntry);
			}
		}
		return Uni.createFrom().voidItem();
	}

}
