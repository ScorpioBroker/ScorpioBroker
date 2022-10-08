package eu.neclab.ngsildbroker.entityhandler.services;

import java.io.IOException;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.collect.ArrayListMultimap;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.AppendEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteAttributeRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.EntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.UpdateEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.results.CreateResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.UpdateResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.EntryCRUDService;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.unchecked.Unchecked;

@Singleton
public class EntityService implements EntryCRUDService {

	@ConfigProperty(name = "scorpio.directDB", defaultValue = "true")
	boolean directDB;
	public static boolean checkEntity = false;

	@Inject
	EntityInfoDAO entityInfoDAO;

	@Inject
	@Channel(AppConstants.ENTITY_CHANNEL)
	Emitter<BaseRequest> kafkaSenderInterface;

	private final static Logger logger = LoggerFactory.getLogger(EntityService.class);

	/**
	 * Method to publish jsonld message to kafka topic
	 * 
	 * @param resolved jsonld message
	 * @param headers
	 * @return RestResponse
	 * @throws KafkaWriteException,Exception
	 * @throws ResponseException
	 */
	public Uni<CreateResult> createEntry(ArrayListMultimap<String, String> headers, Map<String, Object> resolved) {
		logger.debug("createMessage() :: started");
		EntityRequest request;
		try {
			request = new CreateEntityRequest(resolved, headers);
		} catch (ResponseException e) {
			return Uni.createFrom().failure(e);
		}
		return handleRequest(request).onItem().transform(v -> {
			logger.debug("createMessage() :: completed");
			return new CreateResult(request.getId(), true);
		});
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
			Map<String, Object> resolved) {
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
					return handleRequest(t).onItem().transform(v -> {
						logger.trace("partialUpdateEntity() :: completed");
						return t.getUpdateResult();
					});
				});
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
			Map<String, Object> resolved, String[] options) {
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
					return handleRequest(t).onItem().transform(v -> {
						logger.trace("partialUpdateEntity() :: completed");
						return t.getUpdateResult();
					});
				});
	}

	public Uni<Boolean> deleteEntry(ArrayListMultimap<String, String> headers, String entityId) {
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

					return store.onItem().transform(v -> {
						new Thread() {
							@Override
							public void run() {
								kafkaSenderInterface.send(temp);
							}
						}.start();

						return true;
					});

				});
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

}
