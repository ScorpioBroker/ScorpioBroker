package eu.neclab.ngsildbroker.entityhandler.services;

import com.google.common.collect.ArrayListMultimap;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;
import eu.neclab.ngsildbroker.commons.datatypes.requests.*;
import eu.neclab.ngsildbroker.commons.datatypes.results.CreateResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.UpdateResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.EntryCRUDService;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.unchecked.Unchecked;
import io.smallrye.reactive.messaging.annotations.Broadcast;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.jboss.resteasy.reactive.RestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.json.Json;
import java.io.IOException;
import java.net.http.HttpClient;
import java.util.Map;

@Singleton
public class EntityService implements EntryCRUDService {

	private final static Logger logger = LoggerFactory.getLogger(EntityService.class);
	public static boolean checkEntity = false;
	@ConfigProperty(name = "scorpio.directDB", defaultValue = "true")
	boolean directDB;
	@Inject
	EntityInfoDAO entityInfoDAO;
	@Inject
	@Channel(AppConstants.ENTITY_CHANNEL)
	@Broadcast
	Emitter<BaseRequest> kafkaSenderInterface;

	/**
	 * Method to publish jsonld message to kafka topic
	 *
	 * @param resolved jsonld message
	 * @param headers
	 * @return RestResponse
	 * @throws Exception
	 * @throws ResponseException
	 */
	public Uni<CreateResult> createEntry(String tenant, Map<String, Object> resolved, BatchInfo batchInfo) {
		logger.debug("createMessage() :: started");
		EntityRequest request;
		try {
			request = new CreateEntityRequest(resolved, tenant);
		} catch (ResponseException e) {
			return Uni.createFrom().failure(e);
		}
		request.setBatchInfo(batchInfo);
		return handleRequest(request).onItem().transform(v -> {
			logger.debug("createMessage() :: completed");
			return new CreateResult(request.getId(), true);
		});
	}

	public Uni<CreateResult> createEntry(String tenant, Map<String, Object> resolved) {
		return createEntry(tenant, resolved, new BatchInfo(-1, -1));
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
	public Uni<UpdateResult> updateEntry(String tenant, String entityId, Map<String, Object> resolved,
			BatchInfo batchInfo) {
		logger.trace("updateMessage() :: started");
		// get message channel for ENTITY_UPDATE topic

		return EntryCRUDService.validateIdAndGetBody(entityId, tenant, entityInfoDAO).onItem()
				.transform(Unchecked.function(t -> {
					return new UpdateEntityRequest(tenant, entityId, t, resolved, null);
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

	public Uni<UpdateResult> updateEntry(String tenant, String entityId, Map<String, Object> resolved) {
		return updateEntry(tenant, entityId, resolved, new BatchInfo(-1, -1));
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
	public Uni<UpdateResult> appendToEntry(String tenant, String entityId, Map<String, Object> resolved,
			String[] options, BatchInfo batchInfo) {
		logger.trace("appendMessage() :: started");
		// get message channel for ENTITY_APPEND topic
		// payload validation
		if (entityId == null) {
			return Uni.createFrom()
					.failure(new ResponseException(ErrorType.BadRequestData, "empty entity id is not allowed"));
		}

		// get entity details
		return EntryCRUDService.validateIdAndGetBody(entityId, tenant, entityInfoDAO).onItem().transformToUni(t -> {
			AppendEntityRequest req;
			try {
				req = new AppendEntityRequest(tenant, entityId, t, resolved, options);
			} catch (ResponseException e) {
				return Uni.createFrom().failure(e);
			}
			if (req.getUpdateResult().getUpdated().isEmpty()) {
				return Uni.createFrom().item(req.getUpdateResult());
			}
			req.setBatchInfo(batchInfo);
			return handleRequest(req).onItem().transform(v -> {
				logger.trace("partialUpdateEntity() :: completed");
				return req.getUpdateResult();
			});
		});
	}

	public Uni<UpdateResult> appendToEntry(String tenant, String entityId, Map<String, Object> resolved,
			String[] options) {
		return appendToEntry(tenant, entityId, resolved, options, new BatchInfo(-1, -1));
	}

	public Uni<Boolean> deleteEntry(String tenant, String entityId, BatchInfo batchInfo) {
		logger.trace("deleteEntity() :: started");
		if (entityId == null) {
			Uni.createFrom().failure(new ResponseException(ErrorType.BadRequestData, "empty entity id not allowed"));
		}

		return EntryCRUDService.validateIdAndGetBody(entityId, tenant, entityInfoDAO).onItem()
				.transform(Unchecked.function(t -> {
					return new DeleteEntityRequest(entityId, tenant, t);
				})).onItem().transformToUni(t -> {
					Uni<Void> store = entityInfoDAO.storeEntity(t);
					BaseRequest temp = new BaseRequest();
					temp.setTenant(t.getTenant());
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

	public Uni<Boolean> deleteEntry(String tenant, String entityId) {
		return deleteEntry(tenant, entityId, new BatchInfo(-1, -1));
	}

	public Uni<UpdateResult> partialUpdateEntity(String tenant, String entityId, String attrId,
			Map<String, Object> expandedPayload) {
		logger.trace("partialUpdateEntity() :: started");
		// get message channel for ENTITY_APPEND topic
		if (entityId == null) {
			return Uni.createFrom()
					.failure(new ResponseException(ErrorType.BadRequestData, "empty entity id not allowed"));
		}

		// get entity details
		return EntryCRUDService.validateIdAndGetBody(entityId, tenant, entityInfoDAO).onItem().transformToUni(t2 -> {
			UpdateEntityRequest updateEntityRequest;
			try {
				updateEntityRequest = new UpdateEntityRequest(tenant, entityId, t2, expandedPayload, attrId);
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

	public Uni<Boolean> patchToEndPoint(String entityId, ArrayListMultimap<String, String> headers, String payload,
			String attrId) {
		String tenantId = HttpUtils.getInternalTenant(headers);
		return entityInfoDAO.getEndpoint(entityId, tenantId).onItem().transform(endPoint -> {
			if (endPoint != null) {
				WebClient client = WebClient.create(Vertx.vertx());
				client.patchAbs(endPoint + "/ngsi-ld/v1/entities/" + entityId + "/attrs/" + attrId)
						.putHeader(NGSIConstants.TENANT_HEADER, tenantId)
						.putHeader(AppConstants.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON)
						.sendJsonObject(new JsonObject().put("type", "property").put("value", " "), ar -> {
							if (ar.succeeded()) {
								logger.trace("patchToEndPoint() :: completed");
							}
						});
				return true;
			}
			return false;
		});
	}

	public Uni<Boolean> deleteAttribute(String tenant, String entityId, String attrId, String datasetId,
			String deleteAll) {
		logger.trace("deleteAttribute() :: started");
		// get message channel for ENTITY_APPEND topic
		if (entityId == null) {
			return Uni.createFrom()
					.failure(new ResponseException(ErrorType.BadRequestData, "empty entity id not allowed"));
		}

		return EntryCRUDService.validateIdAndGetBody(entityId, tenant, entityInfoDAO).onItem()
				.transform(Unchecked.function(t -> {
					return new DeleteAttributeRequest(tenant, entityId, t, attrId, datasetId, deleteAll);
				})).onItem().transformToUni(this::handleRequest).onItem().transform(t -> true).onFailure()
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
