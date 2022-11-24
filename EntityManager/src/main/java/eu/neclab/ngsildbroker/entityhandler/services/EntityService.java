package eu.neclab.ngsildbroker.entityhandler.services;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import eu.neclab.ngsildbroker.commons.datatypes.results.CreateResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.NGSILDOperationResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.OperationResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.UpdateResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.EntryCRUDService;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.unchecked.Unchecked;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.annotations.Broadcast;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.MultiMap;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.ext.web.client.WebClient;
import io.vertx.mutiny.sqlclient.Row;
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

	private Uni<NGSILDOperationResult> handleDBCreateResult(CreateEntityRequest request, RowSet<Row> resultTable,
			List<Object> originalContext) {
		NGSILDOperationResult result = new NGSILDOperationResult();
		List<Uni<Void>> unis = Lists.newArrayList();
		Map<Integer, Map<String, Object>> hash2Compacted = Maps.newHashMap();
		resultTable.forEach(row -> {

			switch (row.getString(0)) {
			case "ERROR": {
				JsonObject sqlState = ((JsonObject) row.getJson(3));
				if (sqlState.getString("sqlstate").equals(AppConstants.SQL_ALREADY_EXISTS)) {
					result.addFailure(new ResponseException(ErrorType.AlreadyExists));
				} else {
					result.addFailure(new ResponseException(ErrorType.InternalError, sqlState.getString("sqlmessage")));
				}
				break;
			}
			case "ADDED ENTITY": {
				Map<String, Object> entityAdded = ((JsonObject) row.getJson(3)).getMap();
				request.setFinalPayload(entityAdded);

				unis.add(kafkaSenderInterface.send(request).onItem().transform(t -> {
					result.addSuccess(new CreateResult((String) entityAdded.get(NGSIConstants.JSON_LD_ID)));
					return t;
				}));
				break;
			}
			default:
				String host = row.getString(0);
				String tenant = row.getString(1);
				Map<String, Object> entityToForward = ((JsonObject) row.getJson(3)).getMap();
				MultiMap headers = getHeaders((JsonArray) row.getJson(2), request.getHeaders(), tenant);
				String cSourceId = ""; // not in the result for now row.getString(4);
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
							if (failure != null) {
								result.addFailure(new ResponseException(ErrorType.InternalError, failure.getMessage(),
										host, headers, cSourceId));
							} else {
								int statusCode = response.statusCode();
								if (statusCode == 201) {
									result.addSuccess(new CreateResult(
											(String) entityToForward.get(NGSIConstants.JSON_LD_ID), host, headers));
								} else if (statusCode == 207) {
									result.addFailure(
											new ResponseException(statusCode, NGSIConstants.ERROR_MULTI_RESULT_TYPE,
													NGSIConstants.ERROR_MULTI_RESULT_TITLE,
													response.bodyAsJsonObject().getMap(), host, headers, cSourceId));
								} else {
									JsonObject responseBody = response.bodyAsJsonObject();
									if (responseBody == null) {
										result.addFailure(
												new ResponseException(500, NGSIConstants.ERROR_UNEXPECTED_RESULT,
														NGSIConstants.ERROR_UNEXPECTED_RESULT_NULL_TITLE, statusCode,
														host, headers, cSourceId));
									} else {
										if (!responseBody.containsKey(NGSIConstants.ERROR_TYPE)
												|| !responseBody.containsKey(NGSIConstants.ERROR_TITLE)
												|| !responseBody.containsKey(NGSIConstants.ERROR_DETAIL)) {
											result.addFailure(new ResponseException(statusCode,
													responseBody.getString(NGSIConstants.ERROR_TYPE),
													responseBody.getString(NGSIConstants.ERROR_TITLE),
													responseBody.getMap().get(NGSIConstants.ERROR_DETAIL), host,
													headers, cSourceId));
										} else {
											result.addFailure(new ResponseException(500,
													NGSIConstants.ERROR_UNEXPECTED_RESULT,
													NGSIConstants.ERROR_UNEXPECTED_RESULT_NOT_EXPECTED_BODY_TITLE,
													responseBody.getMap(), host, headers, cSourceId));
										}
									}
								}
							}
							return Uni.createFrom().voidItem();
						}));
				break;
			}

		});

		return Uni.combine().all().unis(unis).combinedWith(remoteEntries -> result);
	}

	private MultiMap getHeaders(JsonArray headerFromReg, ArrayListMultimap<String, String> headerFromRequest,
			String tenant) {
		MultiMap result = MultiMap.newInstance(null);
		Set<String> alreadyRemoved = Sets.newHashSet();
		headerFromReg.forEach(t -> {
			JsonObject obj = (JsonObject) t;

			obj.forEach(headerEntry -> {
				String headerName = headerEntry.getKey();
				String headerValue = (String) headerEntry.getValue();
				if (!alreadyRemoved.contains(headerName)) {
					alreadyRemoved.add(headerName);
					headerFromRequest.removeAll(headerName);
				}
				result.add(headerName, headerValue);
			});
		});
		if (tenant != null) {
			headerFromRequest.removeAll(NGSIConstants.TENANT_HEADER);
			result.add(NGSIConstants.TENANT_HEADER, tenant);
		}
		for (Entry<String, String> entry : headerFromRequest.entries()) {
			result.add(entry.getKey(), entry.getValue());
		}
		return result;
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
	public Uni<UpdateResult> updateEntry(ArrayListMultimap<String, String> headers, String entityId,
			Map<String, Object> resolved, BatchInfo batchInfo) {
		logger.trace("updateMessage() :: started");
		// get message channel for ENTITY_UPDATE topic
		String tenantid = HttpUtils.getInternalTenant(headers);
		return EntryCRUDService.validateIdAndGetBody(entityId, tenantid, entityInfoDAO).onItem()
				.transform(Unchecked.function(t -> {
					return new UpdateEntityRequest(headers, entityId, t, resolved, null);
				})).onItem().transformToUni(t -> {
					// if nothing changed just return the result and no publish.
					if (t.getUpdateResult().getUpdated().isEmpty()) {
						return Uni.createFrom().item(t.getUpdateResult());
					}
					t.setBatchInfo(batchInfo);
					return handleRequest(t).onItem().transform(v -> {
						logger.trace("partialUpdateEntity() :: completed");
						return t.getUpdateResult();
					});
				});
	}

	public Uni<UpdateResult> updateEntry(ArrayListMultimap<String, String> headers, String entityId,
			Map<String, Object> resolved) {
		return updateEntry(headers, entityId, resolved, new BatchInfo(-1, -1));
	}

	/**
	 * Method to append fields in existing Entity in system/kafka topic
	 * 
	 * @param entityId - id of entity to be appended
	 * @param resolved - jsonld message containing fileds to be appended
	 * @return AppendResult
	 * @throws ResponseException
	 * @throws IOException
	 */
	public Uni<UpdateResult> appendToEntry(ArrayListMultimap<String, String> headers, String entityId,
			Map<String, Object> resolved, String[] options, BatchInfo batchInfo) {
		logger.trace("appendMessage() :: started");
		// get message channel for ENTITY_APPEND topic
		// payload validation
		if (entityId == null) {
			return Uni.createFrom()
					.failure(new ResponseException(ErrorType.BadRequestData, "empty entity id is not allowed"));
		}

		String tenantId = HttpUtils.getInternalTenant(headers);
		// get entity details
		return EntryCRUDService.validateIdAndGetBody(entityId, tenantId, entityInfoDAO).onItem()
				.transform(Unchecked.function(t -> new AppendEntityRequest(headers, entityId, t, resolved, options)))
				.onItem().transformToUni(t -> {
					if (t.getUpdateResult().getUpdated().isEmpty()) {
						return Uni.createFrom().item(t.getUpdateResult());
					}
					t.setBatchInfo(batchInfo);
					return handleRequest(t).onItem().transform(v -> {
						logger.trace("partialUpdateEntity() :: completed");
						return t.getUpdateResult();
					});
				});
	}

	public Uni<UpdateResult> appendToEntry(ArrayListMultimap<String, String> headers, String entityId,
			Map<String, Object> resolved, String[] options) {
		return appendToEntry(headers, entityId, resolved, options, new BatchInfo(-1, -1));
	}

	public Uni<Boolean> deleteEntry(ArrayListMultimap<String, String> headers, String entityId, BatchInfo batchInfo) {
		logger.trace("deleteEntity() :: started");
		if (entityId == null) {
			Uni.createFrom().failure(new ResponseException(ErrorType.BadRequestData, "empty entity id not allowed"));
		}
		String tenantId = HttpUtils.getInternalTenant(headers);
		return EntryCRUDService.validateIdAndGetBody(entityId, tenantId, entityInfoDAO).onItem()
				.transform(Unchecked.function(t -> {
					return new DeleteEntityRequest(entityId, headers, t);
				})).onItem().transformToUni(t -> {
					Uni<Void> store = entityInfoDAO.storeEntity(t);
					BaseRequest temp = new BaseRequest();
					temp.setHeaders(t.getHeaders());
					temp.setId(t.getId());
					temp.setRequestPayload(t.getOldEntity());
					temp.setFinalPayload(t.getOldEntity());
					temp.setBatchInfo(batchInfo);
					temp.setRequestType(AppConstants.DELETE_REQUEST);
					return store.onItem().transform(v -> {
						kafkaSenderInterface.send(temp);
						return true;
					});

				});
	}

	public Uni<Boolean> deleteEntry(ArrayListMultimap<String, String> headers, String entityId) {
		return deleteEntry(headers, entityId, new BatchInfo(-1, -1));
	}

	public Uni<UpdateResult> partialUpdateEntity(ArrayListMultimap<String, String> headers, String entityId,
			String attrId, Map<String, Object> expandedPayload) {
		logger.trace("partialUpdateEntity() :: started");
		// get message channel for ENTITY_APPEND topic
		if (entityId == null) {
			return Uni.createFrom()
					.failure(new ResponseException(ErrorType.BadRequestData, "empty entity id not allowed"));
		}

		String tenantId = HttpUtils.getInternalTenant(headers);

		// get entity details
		return EntryCRUDService.validateIdAndGetBody(entityId, tenantId, entityInfoDAO).onItem().transformToUni(t2 -> {
			UpdateEntityRequest updateEntityRequest;
			try {
				updateEntityRequest = new UpdateEntityRequest(headers, entityId, t2, expandedPayload, attrId);
			} catch (ResponseException e) {
				return Uni.createFrom().failure(e);

			}
			if (updateEntityRequest.getUpdateResult().getUpdated().isEmpty()) {
				return Uni.createFrom().item(updateEntityRequest.getUpdateResult());
			}
			return handleRequest(updateEntityRequest).onItem().transform(v -> {

				logger.trace("partialUpdateEntity() :: completed");
				return updateEntityRequest.getUpdateResult();
			});
		});
	}

	public Uni<Boolean> deleteAttribute(ArrayListMultimap<String, String> headers, String entityId, String attrId,
			String datasetId, String deleteAll) {
		logger.trace("deleteAttribute() :: started");
		// get message channel for ENTITY_APPEND topic
		if (entityId == null) {
			Uni.createFrom().failure(new ResponseException(ErrorType.BadRequestData, "empty entity id not allowed"));
		}
		String tenantId = HttpUtils.getInternalTenant(headers);
		return EntryCRUDService.validateIdAndGetBody(entityId, tenantId, entityInfoDAO).onItem()
				.transform(Unchecked.function(t -> {
					return new DeleteAttributeRequest(headers, entityId, t, attrId, datasetId, deleteAll);
				})).onItem().transformToUni(t -> handleRequest(t)).onItem().transform(t -> true).onFailure()
				.transform(t -> {
					if (t.getMessage().equals("Attribute is not present"))
						return new ResponseException(ErrorType.NotFound, t.getMessage());
					else
						return t;
				});
	}

	private Uni<Void> handleRequest(EntityRequest request) {
		return entityInfoDAO.storeEntity(request).onItem().transformToUni(t -> {
			request.setSendTimestamp(System.currentTimeMillis());
			kafkaSenderInterface.send(request);
			return Uni.createFrom().voidItem();
		});

	}

	@Override
	public Uni<Void> sendFail(BatchInfo batchInfo) {
		BaseRequest request = new BaseRequest();
		request.setRequestType(AppConstants.BATCH_ERROR_REQUEST);
		request.setBatchInfo(batchInfo);
		request.setId("" + batchInfo.getBatchId());
		return Uni.createFrom().completionStage(kafkaSenderInterface.send(request));
	}

}
