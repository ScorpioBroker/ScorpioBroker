package eu.neclab.ngsildbroker.entityhandler.services;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;
import eu.neclab.ngsildbroker.commons.datatypes.RemoteHost;
import eu.neclab.ngsildbroker.commons.datatypes.requests.AppendEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteAttributeRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.EntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.UpdateEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.results.Attrib;
import eu.neclab.ngsildbroker.commons.datatypes.results.CRUDSuccess;
import eu.neclab.ngsildbroker.commons.datatypes.results.NGSILDOperationResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.EntryCRUDService;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.mutiny.tuples.Tuple3;
import io.smallrye.mutiny.tuples.Tuple4;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.annotations.Broadcast;
import io.vertx.mutiny.core.MultiMap;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.impl.HttpResponseImpl;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.HttpRequest;
import io.vertx.mutiny.ext.web.client.HttpResponse;
import io.vertx.mutiny.ext.web.client.WebClient;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowIterator;
import io.vertx.mutiny.sqlclient.RowSet;

@Singleton
public class EntityService implements EntryCRUDService {

	private final static Logger logger = LoggerFactory.getLogger(EntityService.class);

	@ConfigProperty(name = "scorpio.directDB", defaultValue = "true")
	boolean directDB;
	public static boolean checkEntity = false;

	@Inject
	EntityInfoDAO entityDAO;

	@Inject
	@Channel(AppConstants.ENTITY_CHANNEL)
	@Broadcast
	MutinyEmitter<BaseRequest> kafkaSenderInterface;

	@Inject
	Vertx vertx;

	WebClient webClient;

	private JsonLdOptions opts = new JsonLdOptions(JsonLdOptions.JSON_LD_1_1);

	private Random random = new Random();

	@PostConstruct
	void init() {
		webClient = WebClient.create(vertx);
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
									return handleWebResponse(localResult, response, failure, ArrayUtils.toArray(201),
											remoteHost, HttpUtils.getAttribsFromCompactedPayload(entry.getValue()));
								}));
					} else {
						unis.add(webClient.post(remoteHost.host() + NGSIConstants.ENDPOINT_BATCH_CREATE)
								.putHeaders(remoteHost.headers())
								.sendJson(new JsonArray(Lists.newArrayList(new JsonObject(entry.getValue()))))
								.onItemOrFailure().transformToUni((response, failure) -> {
									return handleWebResponse(localResult, response, failure, ArrayUtils.toArray(201),
											remoteHost, HttpUtils.getAttribsFromCompactedPayload(entry.getValue()));
								}));
					}
				}
				return Uni.combine().all().unis(unis).combinedWith(t -> localResult);
			});

		});
	}

	public Uni<List<NGSILDOperationResult>> createMultipleEntry(String tenant, List<Map<String, Object>> entities,
			Context originalContext) {
		logger.debug("createMessage() :: started");
		BatchInfo batchInfo = new BatchInfo(random.nextInt(), entities.size());

		List<Uni<Tuple2<NGSILDOperationResult, Map<RemoteHost, Map<String, Object>>>>> dbUnis = Lists.newArrayList();
		for (Map<String, Object> entity : entities) {
			CreateEntityRequest request = new CreateEntityRequest(tenant, entity, batchInfo);
			dbUnis.add(entityDAO.createEntity(request).onItem().transformToUni(resultTable -> {
				if (resultTable.size() == 0) {
					return Uni.createFrom().failure(new ResponseException(ErrorType.InternalError,
							"No result from the database this should never happen"));
				}
				return handleDBCreateResult(request, resultTable, originalContext);
			}));
		}

		return Uni.combine().all().unis(dbUnis).combinedWith(list -> {

			ArrayListMultimap<RemoteHost, Map<String, Object>> remoteHost2Entities = ArrayListMultimap.create();
			Map<String, NGSILDOperationResult> entityId2Result = Maps.newHashMap();

			for (Object obj : list) {
				Tuple2<NGSILDOperationResult, Map<RemoteHost, Map<String, Object>>> tmp = (Tuple2<NGSILDOperationResult, Map<RemoteHost, Map<String, Object>>>) obj;
				NGSILDOperationResult result = tmp.getItem1();
				Map<RemoteHost, Map<String, Object>> remoteHost2Entity = tmp.getItem2();
				for (Entry<RemoteHost, Map<String, Object>> entry : remoteHost2Entities.entries()) {
					remoteHost2Entities.put(entry.getKey(), entry.getValue());
				}
				entityId2Result.put(tmp.getItem1().getEntityId(), tmp.getItem1());
			}
			// collect all single entities for remote batch operations
			return Tuple2.of(remoteHost2Entities, entityId2Result);
		}).onItem().transformToUni(tuple -> {
			ArrayListMultimap<RemoteHost, Map<String, Object>> remoteHost2Entities = tuple.getItem1();

			List<Map<String, Object>> remoteEntities;
			List<Uni<Tuple3<List<NGSILDOperationResult>, RemoteHost, List<Map<String, Object>>>>> unis = new ArrayList<>(
					remoteHost2Entities.size());
			for (RemoteHost remoteHost : remoteHost2Entities.keys()) {
				remoteEntities = remoteHost2Entities.get(remoteHost);
				if (remoteHost.canDoBatchOp()) {
					unis.add(webClient.post(remoteHost.host() + NGSIConstants.ENDPOINT_BATCH_CREATE)
							.putHeaders(remoteHost.headers()).sendJson(new JsonArray(remoteEntities)).onItemOrFailure()
							.transform((response, failure) -> {
								return Tuple3.of(handleBatchResponse(response, failure), remoteHost, remoteEntities);
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
							handleWebResponse(tmpResult, next.getItem1(), next.getItem2(), ArrayUtils.toArray(201),
									remoteHost, HttpUtils.getAttribsFromCompactedPayload(next.getItem4()));
							opResults.add(tmpResult);
						}
						return Tuple3.of(opResults, next.getItem3(), postedEntityList);
					}));
				}
			}
			return Uni.combine().all().unis(unis).combinedWith(t -> {
				Map<String, NGSILDOperationResult> entityId2Result = tuple.getItem2();
				for (Object obj : t) {
					Tuple3<List<NGSILDOperationResult>, RemoteHost, List<Map<String, Object>>> tmp = (Tuple3<List<NGSILDOperationResult>, RemoteHost, List<Map<String, Object>>>) obj;
					mergeRemoteResults(entityId2Result, tmp);
				}
				return Lists.newArrayList(entityId2Result.values());
			});

		});

	}

	private void mergeRemoteResults(Map<String, NGSILDOperationResult> entityId2Result,
			Tuple3<List<NGSILDOperationResult>, RemoteHost, List<Map<String, Object>>> tmp) {
		// TODO Auto-generated method stub

	}

	private List<NGSILDOperationResult> handleBatchResponse(HttpResponse<Buffer> response, Throwable failure) {
		// TODO Auto-generated method stub
		return null;
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
									return handleWebResponse(localResult, response, failure, ArrayUtils.toArray(201),
											remoteHost, HttpUtils.getAttribsFromCompactedPayload(entry.getValue()));
								}));
					}
				}

				return localResult;

			});
		});
	}

	/**
	 * 
	 * @param request         the reate request
	 * @param resultTable     the RowSet coming from the DB
	 * @param originalContext the context used in the original request
	 * @return a Tuple of the local db result and remote hosts (tenant host headers)
	 */
	private Uni<Tuple2<NGSILDOperationResult, Map<RemoteHost, Map<String, Object>>>> handleDBCreateResult(
			CreateEntityRequest request, RowSet<Row> resultTable, Context originalContext) {
		NGSILDOperationResult operationResult = new NGSILDOperationResult(AppConstants.CREATE_REQUEST, request.getId());
		Map<RemoteHost, Map<String, Object>> remoteResults = Maps.newHashMap();
		List<Uni<Void>> unis = Lists.newArrayList();
		Map<Integer, Map<String, Object>> hash2Compacted = Maps.newHashMap();
		Context context = JsonLdProcessor.getCoreContextClone().parse(originalContext, true);
		RowIterator<Row> it = resultTable.iterator();
		Row row;
		Uni<Void> sendKafka = Uni.createFrom().voidItem();
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
						compacted = JsonLdProcessor.compact(entityToForward, null, originalContext, opts, -1);
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

		return sendKafka.onItem().transform(v -> Tuple2.of(operationResult, remoteResults));
	}

	private Uni<Void> handleWebResponse(NGSILDOperationResult result, HttpResponse<Buffer> response, Throwable failure,
			Integer[] successCode, RemoteHost host, Set<Attrib> attribs) {
		if (failure != null) {
			result.addFailure(new ResponseException(ErrorType.InternalError, failure.getMessage(), host, attribs));
		} else {
			int statusCode = response.statusCode();
			if (ArrayUtils.contains(successCode, statusCode)) {
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
	 * @param resolved - jsonld message containing fileds to be updated with updated
	 *                 values
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
			return handleDBUpdateResult(request, resultTable, originalContext);
		});
	}

	private Uni<NGSILDOperationResult> handleDBUpdateResult(EntityRequest request, RowSet<Row> resultTable,
			Context originalContext) {
		NGSILDOperationResult result = new NGSILDOperationResult(AppConstants.UPDATE_REQUEST, request.getId());
		List<Uni<Void>> unis = Lists.newArrayList();
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
				String url;
				HttpRequest<Buffer> req;
				if (request.getRequestType() == AppConstants.UPDATE_REQUEST) {
					if (((UpdateEntityRequest) request).getAttrName() != null) {
						req = webClient.patch(host + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "/" + request.getId()
								+ "/attrs/" + ((UpdateEntityRequest) request).getAttrName());
					} else {
						req = webClient.patch(
								host + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "/" + request.getId() + "/attrs");
					}
				} else {
					req = webClient
							.post(host + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "/" + request.getId() + "/attrs");
				}

				unis.add(req.putHeaders(headers).sendJsonObject(new JsonObject(compacted)).onItemOrFailure()
						.transformToUni((response, failure) -> {
							return handleWebResponse(result, response, failure, 201, host, headers, cSourceId,
									entityToForward, context);
						}));
				break;
			}

		}
		if (!internalSend) {
			unis.add(sendFail(request.getBatchInfo()));
		}
		return Uni.combine().all().unis(unis).combinedWith(remoteEntries -> result);
	}

	@Override
	public Uni<Void> sendFail(BatchInfo batchInfo) {
		BaseRequest request = new BaseRequest();
		request.setRequestType(AppConstants.BATCH_ERROR_REQUEST);
		request.setBatchInfo(batchInfo);
		request.setId("" + batchInfo.getBatchId());
		return kafkaSenderInterface.send(request);
	}

	@Override
	public Uni<NGSILDOperationResult> updateEntry(String tenant, String entityId, Map<String, Object> entry,
			Context originalContext) {
		return updateEntry(tenant, entityId, entry, originalContext, new BatchInfo(-1, -1));
	}

	@Override
	public Uni<NGSILDOperationResult> appendToEntry(String tenant, String entityId, Map<String, Object> entry,
			String[] options, Context originalContext) {
		AppendEntityRequest request = new AppendEntityRequest(tenant, entityId, entry, null);
		boolean noOverwrite = Arrays.stream(options).anyMatch(NGSIConstants.NO_OVERWRITE_OPTION::equals);
		return entityDAO.appendEntity(request, noOverwrite).onItem().transformToUni(resultTable -> {
			if (resultTable.size() == 0) {
				return Uni.createFrom().failure(new ResponseException(ErrorType.InternalError,
						"No result from the database this should never happen"));
			}
			return handleDBUpdateResult(request, resultTable, originalContext);
		});
	}

	@Override
	public Uni<NGSILDOperationResult> deleteEntry(String tenant, String entityId, Context originalContext) {
		DeleteEntityRequest request = new DeleteEntityRequest(tenant, entityId, null);
		return entityDAO.deleteEntity(request).onItem().transformToUni(resultTable -> {
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

					StringBuilder url = new StringBuilder(
							host + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "/" + request.getId());
					unis.add(webClient.delete(url.toString()).putHeaders(remoteHeaders).send().onItemOrFailure()
							.transformToUni((response, failure) -> {
								return handleWebResponse(result, response, failure, 204, host, remoteHeaders, cSourceId,
										entityToForward, context);
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
			return handleDBUpdateResult(request, resultTable, originalContext);
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

					unis.add(webClient.delete(url.toString()).putHeaders(remoteHeaders).send().onItemOrFailure()
							.transformToUni((response, failure) -> {
								return handleWebResponse(result, response, failure, 204, host, remoteHeaders, cSourceId,
										entityToForward, context);
							}));
					break;
				}

			});

			return Uni.combine().all().unis(unis).combinedWith(remoteEntries -> result);

		});
	}

}
