package eu.neclab.ngsildbroker.entityhandler.services;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;

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
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.annotations.Broadcast;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.MultiMap;
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

	@PostConstruct
	void init() {
		webClient = WebClient.create(vertx);
	}

	/**
	 * Method to publish jsonld message to kafka topic
	 * 
	 * @param resolved jsonld message
	 * @param headers
	 * @return RestResponse
	 * @throws KafkaWriteException,Exception
	 * @throws ResponseException
	 */
	public Uni<NGSILDOperationResult> createEntry(ArrayListMultimap<String, String> headers,
			Map<String, Object> resolved, List<Object> originalContext, BatchInfo batchInfo) {
		logger.debug("createMessage() :: started");
		CreateEntityRequest request = new CreateEntityRequest(resolved, headers, batchInfo);
		return entityDAO.createEntity(request).onItem().transformToUni(resultTable -> {
			if (resultTable.size() == 0) {
				return Uni.createFrom().failure(new ResponseException(ErrorType.InternalError,
						"No result from the database this should never happen"));
			}
			return handleDBCreateResult(request, resultTable, originalContext);
		});
	}

	public Uni<NGSILDOperationResult> upsertEntry(ArrayListMultimap<String, String> headers,
			Map<String, Object> resolved, List<Object> originalContext, BatchInfo batchInfo) {
		logger.debug("createMessage() :: started");
		CreateEntityRequest request = new CreateEntityRequest(resolved, headers, batchInfo);
		return entityDAO.upsertEntity(request).onItem().transformToUni(resultTable -> {
			if (resultTable.size() == 0) {
				return Uni.createFrom().failure(new ResponseException(ErrorType.InternalError,
						"No result from the database this should never happen"));
			}
			return handleDBCreateResult(request, resultTable, originalContext);
		});
	}

	private Uni<NGSILDOperationResult> handleDBCreateResult(CreateEntityRequest request, RowSet<Row> resultTable,
			List<Object> originalContext) {
		NGSILDOperationResult result = new NGSILDOperationResult(AppConstants.CREATE_REQUEST, request.getId());
		List<Uni<Void>> unis = Lists.newArrayList();
		Map<Integer, Map<String, Object>> hash2Compacted = Maps.newHashMap();
		Context context = JsonLdProcessor.getCoreContextClone().parse(originalContext, true);
		boolean internalSend = false;
		RowIterator<Row> it = resultTable.iterator();
		while (it.hasNext()) {
			Row row = it.next();
			switch (row.getString(0)) {
			case "ERROR": {
				String sqlState = row.getString(1);
				JsonObject entity = row.getJsonObject(3);
				if (sqlState.equals(AppConstants.SQL_ALREADY_EXISTS)) {
					result.addFailure(new ResponseException(ErrorType.AlreadyExists, ErrorType.AlreadyExists.getTitle(),
							entity.getMap(), context));
				} else {
					result.addFailure(new ResponseException(ErrorType.InternalError, row.getString(2)));
				}
				break;
			}
			case "ADDED ENTITY": {
				Map<String, Object> entityAdded = ((JsonObject) row.getJson(3)).getMap();
				request.setPayload(entityAdded);
				internalSend = true;
				unis.add(kafkaSenderInterface.send(request).onItem().transform(t -> {
					result.addSuccess(new CRUDSuccess(null, null, null, entityAdded, context));
					return t;
				}));
				break;
			}
			default:
				String host = row.getString(0);
				String tenant = row.getString(1);
				Map<String, Object> entityToForward = ((JsonObject) row.getJson(3)).getMap();
				MultiMap headers = HttpUtils.getHeaders((JsonArray) row.getJson(2), request.getHeaders(), tenant);
				String cSourceId = row.getString(4);
				int hash = entityToForward.hashCode();
				Map<String, Object> compacted = hash2Compacted.get(hash);
				if (compacted == null) {
					try {
						compacted = JsonLdProcessor.compact(entityToForward, originalContext, opts);
					} catch (ResponseException e) {
						// TODO add host info
						result.addFailure(e);
					} catch (Exception e) {
						// TODO add host info
						result.addFailure(new ResponseException(ErrorType.InternalError, e.getMessage()));
					}
					hash2Compacted.put(hash, compacted);
				}
				unis.add(webClient.post(host + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT).putHeaders(headers)
						.sendJsonObject(new JsonObject(compacted)).onItemOrFailure()
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

	private Uni<Void> handleWebResponse(NGSILDOperationResult result, HttpResponse<Buffer> response, Throwable failure,
			int successCode, String host, MultiMap headers, String cSourceId, Map<String, Object> entityToForward,
			Context context) {
		if (failure != null) {
			result.addFailure(new ResponseException(ErrorType.InternalError, failure.getMessage(), host, headers,
					cSourceId, entityToForward, context));
		} else {
			int statusCode = response.statusCode();
			if (statusCode == successCode) {
				result.addSuccess(new CRUDSuccess(host, headers, cSourceId, entityToForward, context));
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
					result.addFailure(new ResponseException(500, NGSIConstants.ERROR_UNEXPECTED_RESULT,
							NGSIConstants.ERROR_UNEXPECTED_RESULT_NULL_TITLE, statusCode, host, headers, cSourceId,
							entityToForward, context));
				} else {
					if (!responseBody.containsKey(NGSIConstants.ERROR_TYPE)
							|| !responseBody.containsKey(NGSIConstants.ERROR_TITLE)
							|| !responseBody.containsKey(NGSIConstants.ERROR_DETAIL)) {
						result.addFailure(
								new ResponseException(statusCode, responseBody.getString(NGSIConstants.ERROR_TYPE),
										responseBody.getString(NGSIConstants.ERROR_TITLE),
										responseBody.getMap().get(NGSIConstants.ERROR_DETAIL), host, headers, cSourceId,
										entityToForward, context));
					} else {
						result.addFailure(new ResponseException(500, NGSIConstants.ERROR_UNEXPECTED_RESULT,
								NGSIConstants.ERROR_UNEXPECTED_RESULT_NOT_EXPECTED_BODY_TITLE, responseBody.getMap(),
								host, headers, cSourceId, entityToForward, context));
					}
				}
			}
		}
		return Uni.createFrom().voidItem();
	}

	public Uni<NGSILDOperationResult> createEntry(ArrayListMultimap<String, String> headers,
			Map<String, Object> resolved, List<Object> originalContext) {
		return createEntry(headers, resolved, originalContext, new BatchInfo(-1, -1));
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
	public Uni<NGSILDOperationResult> updateEntry(ArrayListMultimap<String, String> headers, String entityId,
			Map<String, Object> payload, List<Object> originalContext, BatchInfo batchInfo) {
		logger.trace("updateMessage() :: started");
		UpdateEntityRequest request = new UpdateEntityRequest(headers, entityId, payload, null, batchInfo);
		return entityDAO.updateEntity(request).onItem().transformToUni(resultTable -> {
			if (resultTable.size() == 0) {
				return Uni.createFrom().failure(new ResponseException(ErrorType.InternalError,
						"No result from the database this should never happen"));
			}
			return handleDBUpdateResult(request, resultTable, originalContext);
		});
	}

	private Uni<NGSILDOperationResult> handleDBUpdateResult(EntityRequest request, RowSet<Row> resultTable,
			List<Object> originalContext) {
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
				MultiMap headers = HttpUtils.getHeaders((JsonArray) row.getJson(2), request.getHeaders(), tenant);
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
	public Uni<NGSILDOperationResult> updateEntry(ArrayListMultimap<String, String> headers, String entityId,
			Map<String, Object> entry, List<Object> originalContext) {
		return updateEntry(headers, entityId, entry, originalContext, new BatchInfo(-1, -1));
	}

	@Override
	public Uni<NGSILDOperationResult> appendToEntry(ArrayListMultimap<String, String> headers, String entityId,
			Map<String, Object> entry, String[] options, List<Object> originalContext) {
		return appendToEntry(headers, entityId, entry, options, originalContext, new BatchInfo(-1, -1));

	}

	@Override
	public Uni<NGSILDOperationResult> appendToEntry(ArrayListMultimap<String, String> headers, String entityId,
			Map<String, Object> entry, String[] options, List<Object> originalContext, BatchInfo batchInfo) {
		AppendEntityRequest request = new AppendEntityRequest(headers, entityId, entry, batchInfo);
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
	public Uni<NGSILDOperationResult> deleteEntry(ArrayListMultimap<String, String> headers, String entryId,
			List<Object> originalContext) {
		return deleteEntry(headers, entryId, originalContext, new BatchInfo(-1, -1));
	}

	@Override
	public Uni<NGSILDOperationResult> deleteEntry(ArrayListMultimap<String, String> headers, String entityId,
			List<Object> originalContext, BatchInfo batchInfo) {
		DeleteEntityRequest request = new DeleteEntityRequest(headers, entityId, batchInfo);
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
					String tenant = row.getString(1);
					// This is not a final solution ... but in the case of full success on the other
					// side there is no way of know what has been deleted so ... don't know how to
					// handle that
					Map<String, Object> entityToForward = Maps.newHashMap();
					MultiMap remoteHeaders = HttpUtils.getHeaders((JsonArray) row.getJson(2), request.getHeaders(),
							tenant);
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

	public Uni<NGSILDOperationResult> partialUpdateEntity(ArrayListMultimap<String, String> headers, String entityId,
			String attribName, Map<String, Object> payload, List<Object> originalContext) {
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
		UpdateEntityRequest request = new UpdateEntityRequest(headers, entityId, effectivePayload, attribName,
				new BatchInfo(-1, -1));
		return entityDAO.partialUpdateEntity(request).onItem().transformToUni(resultTable -> {
			if (resultTable.size() == 0) {
				return Uni.createFrom().failure(new ResponseException(ErrorType.InternalError,
						"No result from the database this should never happen"));
			}
			return handleDBUpdateResult(request, resultTable, originalContext);
		});
	}

	public Uni<NGSILDOperationResult> deleteAttribute(ArrayListMultimap<String, String> headers, String entityId,
			String attribName, String datasetId, boolean deleteAll, List<Object> originalContext) {
		logger.trace("updateMessage() :: started");
		// place the id in the payload to be sure

		DeleteAttributeRequest request = new DeleteAttributeRequest(headers, entityId, attribName, datasetId,
				deleteAll);
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
					String tenant = row.getString(1);
					Map<String, Object> entityToForward = Maps.newHashMap();
					Map<String, Object> dummy = Maps.newHashMap();
					entityToForward.put(attribName, Lists.newArrayList(dummy));
					MultiMap remoteHeaders = HttpUtils.getHeaders((JsonArray) row.getJson(2), request.getHeaders(),
							tenant);
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
