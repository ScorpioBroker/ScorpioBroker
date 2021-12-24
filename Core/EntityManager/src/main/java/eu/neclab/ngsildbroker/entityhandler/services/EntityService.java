package eu.neclab.ngsildbroker.entityhandler.services;

import java.io.IOException;
import java.sql.SQLTransientConnectionException;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Random;

import javax.annotation.PostConstruct;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.datatypes.requests.AppendEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteAttributeRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.EntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.UpdateEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.results.AppendResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.UpdateResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.EntryCRUDService;
import eu.neclab.ngsildbroker.commons.storage.StorageWriterDAO;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;

@Service
@EnableAutoConfiguration
@EnableKafka
public class EntityService implements EntryCRUDService {


	@Value("${entity.create.topic}")
	String ENTITY_CREATE_TOPIC;
	@Value("${entity.append.topic}")
	String ENTITY_APPEND_TOPIC;
	@Value("${entity.update.topic}")
	String ENTITY_UPDATE_TOPIC;
	@Value("${entity.delete.topic}")
	String ENTITY_DELETE_TOPIC;

	boolean directDB = true;
	public static boolean checkEntity = false;
	@Autowired
	@Qualifier("emdao")
	StorageWriterDAO storageWriterDao;

	@Autowired
	EntityInfoDAO entityInfoDAO;

	@Autowired
	KafkaTemplate<String, Object> kafkaTemplate;

	LocalDateTime startAt;
	LocalDateTime endAt;
	private ArrayListMultimap<String, String> entityIds = ArrayListMultimap.create();
	private final static Logger logger = LogManager.getLogger(EntityService.class);

	// construct in-memory
	@PostConstruct
	private void loadStoredEntitiesDetails() throws ResponseException {
		synchronized (this.entityIds) {
			this.entityIds = entityInfoDAO.getAllIds();
		}
		logger.trace("filling in-memory hashmap completed:");
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
	public String createEntry(ArrayListMultimap<String, String> headers, Map<String, Object> resolved)
			throws ResponseException, Exception {
		// get message channel for ENTITY_CREATE topic.
		logger.debug("createMessage() :: started");
		// MessageChannel messageChannel = producerChannels.createWriteChannel();
		EntityRequest request = new CreateEntityRequest(resolved, headers);

		String tenantId = HttpUtils.getInternalTenant(headers);

		synchronized (this.entityIds) {
			if (this.entityIds.containsEntry(tenantId, request.getId())) {
				throw new ResponseException(ErrorType.AlreadyExists, request.getId() + " already exists");
			}
			this.entityIds.put(tenantId, request.getId());
		}
		pushToDB(request);
		sendToKafka(ENTITY_CREATE_TOPIC, request);

		logger.debug("createMessage() :: completed");
		return request.getId();
	}

	private void sendToKafka(String topic, BaseRequest request) {
		new Thread() {
			public void run() {
				kafkaTemplate.send(topic, request.getId(), new BaseRequest(request));
			};
		}.start();
	}

	private void pushToDB(EntityRequest request) {
		boolean success = false;
		while (!success) {
			try {
				logger.debug("Received message: " + request.getWithSysAttrs());
				logger.trace("Writing data...");
				if (storageWriterDao != null && storageWriterDao.storeEntity(request)) {

					logger.trace("Writing is complete");
				}
				success = true;
			} catch (SQLTransientConnectionException e) {
				logger.warn("SQL Exception attempting retry");
				Random random = new Random();
				int randomNumber = random.nextInt(4000) + 500;
				try {
					Thread.sleep(randomNumber);
				} catch (InterruptedException e1) {
					logger.error(e1);
				}
			}
		}
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
	public UpdateResult updateEntry(ArrayListMultimap<String, String> headers, String entityId,
			Map<String, Object> resolved) throws ResponseException, Exception {
		logger.trace("updateMessage() :: started");
		// get message channel for ENTITY_UPDATE topic

		String tenantid = HttpUtils.getInternalTenant(headers);
		// get entity details
		Map<String, Object> entityBody = validateIdAndGetBody(entityId, tenantid);
		// String entityBody = validateIdAndGetBody(entityId);
		UpdateEntityRequest request = new UpdateEntityRequest(headers, entityId, entityBody, resolved, null);

		// update fields

		// pubilsh merged message
		// & check if anything is changed.
		if (request.getStatus()) {
			if (directDB) {
				pushToDB(request);
			}
			sendToKafka(ENTITY_UPDATE_TOPIC, request);
		}
		logger.trace("updateMessage() :: completed");
		return request.getUpdateResult();
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
	public AppendResult appendToEntry(ArrayListMultimap<String, String> headers, String entityId,
			Map<String, Object> resolved, String[] options) throws ResponseException, Exception {
		logger.trace("appendMessage() :: started");
		// get message channel for ENTITY_APPEND topic
		// payload validation
		if (entityId == null) {
			throw new ResponseException(ErrorType.BadRequestData, "empty entity id is not allowed");
		}

		String tenantId = HttpUtils.getInternalTenant(headers);
		// get entity details
		Map<String, Object> entityBody = validateIdAndGetBody(entityId, tenantId);
		AppendEntityRequest request = new AppendEntityRequest(headers, entityId, entityBody, resolved, options);
		// get entity from ENTITY topic.
		// pubilsh merged message
		// check if anything is changed

		if (directDB) {
			pushToDB(request);
		}
		sendToKafka(ENTITY_APPEND_TOPIC, request);

		logger.trace("appendMessage() :: completed");
		return request.getAppendResult();
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> validateIdAndGetBody(String entityId, String tenantId) throws ResponseException {
		// null id check
		if (entityId == null) {
			throw new ResponseException(ErrorType.BadRequestData, "empty entity id not allowed");
		}
		synchronized (this.entityIds) {
			if (!this.entityIds.containsKey(tenantId)) {
				throw new ResponseException(ErrorType.TenantNotFound, "tenant " + tenantId + " not found");
			}
			if (!this.entityIds.containsValue(entityId)) {
				throw new ResponseException(ErrorType.NotFound, "Entity Id " + entityId + " not found");
			}
		}
		String entityBody = null;
		if (directDB) {
			entityBody = this.entityInfoDAO.getEntity(entityId, tenantId);
		} else {
			// todo add back storage manager calls
		}

		try {
			return (Map<String, Object>) JsonUtils.fromString(entityBody);
		} catch (IOException e) {
			throw new AssertionError("can't load internal json");
		}
	}

	public boolean deleteEntry(ArrayListMultimap<String, String> headers, String entityId)
			throws ResponseException, Exception {
		logger.trace("deleteEntity() :: started");
		// get message channel for ENTITY_DELETE topic
		if (entityId == null) {
			throw new ResponseException(ErrorType.BadRequestData, "empty entity id not allowed");
		}
		String tenantId = HttpUtils.getInternalTenant(headers);
		synchronized (this.entityIds) {

			if (!this.entityIds.containsEntry(tenantId, entityId)) {
				throw new ResponseException(ErrorType.NotFound, entityId + " not found");
			}
			this.entityIds.remove(tenantId, entityId);

		}
		Map<String, Object> oldEntity = (Map<String, Object>)JsonUtils.fromString(entityInfoDAO.getEntity(entityId, tenantId));

		EntityRequest request = new DeleteEntityRequest(entityId, headers);
		if (directDB) {
			pushToDB(request);
		}
		request.setRequestPayload(oldEntity);
		request.setFinalPayload(oldEntity);
		sendToKafka(ENTITY_DELETE_TOPIC, request);
		logger.trace("deleteEntity() :: completed");
		return true;
	}

	public UpdateResult partialUpdateEntity(ArrayListMultimap<String, String> headers, String entityId, String attrId,
			Map<String, Object> expandedPayload) throws ResponseException, Exception {
		logger.trace("partialUpdateEntity() :: started");
		// get message channel for ENTITY_APPEND topic
		if (entityId == null) {
			throw new ResponseException(ErrorType.BadRequestData, "empty entity id not allowed");
		}

		String tenantid = HttpUtils.getInternalTenant(headers);

		// get entity details
		Map<String, Object> entityBody = validateIdAndGetBody(entityId, tenantid);

		// JsonNode originalJsonNode = objectMapper.readTree(originalJson);
		UpdateEntityRequest request = new UpdateEntityRequest(headers, entityId, entityBody, expandedPayload, attrId);
		// pubilsh merged message
		// check if anything is changed.
		if (request.getStatus()) {
			if (directDB) {
				pushToDB(request);
			}
			sendToKafka(ENTITY_UPDATE_TOPIC, request);
		}
		logger.trace("partialUpdateEntity() :: completed");
		return request.getUpdateResult();
	}

	public boolean deleteAttribute(ArrayListMultimap<String, String> headers, String entityId, String attrId,
			String datasetId, String deleteAll) throws ResponseException, Exception {
		logger.trace("deleteAttribute() :: started");
		// get message channel for ENTITY_APPEND topic

		if (entityId == null) {
			throw new ResponseException(ErrorType.BadRequestData, "empty entity id not allowed");
		}
		String tenantid = HttpUtils.getInternalTenant(headers);
		// get entity details
		Map<String, Object> entityBody = validateIdAndGetBody(entityId, tenantid);
		// get entity details from in-memory hashmap

		DeleteAttributeRequest request = new DeleteAttributeRequest(headers, entityId, entityBody, attrId, datasetId,
				deleteAll);
		if (directDB) {
			pushToDB(request);
		}
		sendToKafka(ENTITY_DELETE_TOPIC, request);
		logger.trace("deleteAttribute() :: completed");
		return true;
	}

}
