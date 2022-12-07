package eu.neclab.ngsildbroker.entityhandler.services;

import java.io.IOException;
import java.sql.SQLTransientConnectionException;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;

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
import eu.neclab.ngsildbroker.commons.datatypes.results.UpdateResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.EntryCRUDService;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;

import org.springframework.web.client.RestTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;

@Service
@EnableAutoConfiguration
@EnableKafka
public class EntityService implements EntryCRUDService {

	@Value("${scorpio.topics.entity}")
	private String ENTITY_TOPIC;
	@Value("${scorpio.directDB}")
	boolean directDB;
	public static boolean checkEntity = false;

	@Autowired
	EntityService entityService;

	@Autowired
	EntityInfoDAO entityInfoDAO;

	@Autowired
	KafkaTemplate<String, Object> kafkaTemplate;

	private ThreadPoolExecutor kafkaExecutor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.MINUTES,
			new LinkedBlockingQueue<Runnable>());

	LocalDateTime startAt;
	LocalDateTime endAt;
	private ArrayListMultimap<String, String> entityIds = ArrayListMultimap.create();
	private final static Logger logger = LogManager.getLogger(EntityService.class);

	/**
	 * Method to publish jsonld message to kafka topic
	 * 
	 * @param resolved jsonld message
	 * @param headers
	 * @return RestResponse
	 * @throws KafkaWriteException,Exception
	 * @throws ResponseException
	 */
	public CreateResult createEntry(ArrayListMultimap<String, String> headers, Map<String, Object> resolved,
			BatchInfo batchInfo) throws ResponseException, Exception {

		// get message channel for ENTITY_CREATE topic.
		logger.debug("createMessage() :: started");
		// MessageChannel messageChannel = producerChannels.createWriteChannel();
		EntityRequest request = new CreateEntityRequest(resolved, headers);
		request.setBatchInfo(batchInfo);
		pushToDB(request);
		sendToKafka(request);
		logger.debug("createMessage() :: completed");
		return new CreateResult(request.getId(), true);
	}

	@Override
	public CreateResult createEntry(ArrayListMultimap<String, String> headers, Map<String, Object> resolved)
			throws ResponseException, Exception {
		// TODO Auto-generated method stub
		return createEntry(headers, resolved, new BatchInfo(-1, -1));
	}

	private void sendToKafka(BaseRequest request) {
		kafkaExecutor.execute(new Runnable() {
			@Override
			public void run() {
				kafkaTemplate.send(ENTITY_TOPIC, request.getId(), new BaseRequest(request));

			}
		});
	}

	private void pushToDB(EntityRequest request) throws ResponseException {
		boolean success = false;
		while (!success) {
			try {
				logger.debug("Received message: " + request.getWithSysAttrs());
				logger.trace("Writing data...");
				if (entityInfoDAO != null && entityInfoDAO.storeEntity(request)) {

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
			Map<String, Object> resolved, BatchInfo batchInfo) throws ResponseException, Exception {
		logger.trace("updateMessage() :: started");
		// get message channel for ENTITY_UPDATE topic

		String tenantid = HttpUtils.getInternalTenant(headers);
		// get entity details
		Map<String, Object> entityBody = validateIdAndGetBody(entityId, tenantid);
		// String entityBody = validateIdAndGetBody(entityId);
		UpdateEntityRequest request = new UpdateEntityRequest(headers, entityId, entityBody, resolved, null);
		request.setBatchInfo(batchInfo);
		// update fields

		// pubilsh merged message
		// & check if anything is changed.
		if (!request.getUpdateResult().getUpdated().isEmpty()) {
			handleRequest(request);
		}
		logger.trace("updateMessage() :: completed");
		return request.getUpdateResult();
	}

	public UpdateResult updateEntry(ArrayListMultimap<String, String> headers, String entityId,
			Map<String, Object> resolved) throws ResponseException, Exception {
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
	public UpdateResult appendToEntry(ArrayListMultimap<String, String> headers, String entityId,
			Map<String, Object> resolved, String[] options, BatchInfo batchInfo) throws ResponseException, Exception {
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
		request.setBatchInfo(batchInfo);
		handleRequest(request);

		logger.trace("appendMessage() :: completed");
		return request.getUpdateResult();
	}

	public UpdateResult appendToEntry(ArrayListMultimap<String, String> headers, String entityId,
			Map<String, Object> resolved, String[] options) throws ResponseException, Exception {
		return appendToEntry(headers, entityId, resolved, options, new BatchInfo(-1, -1));
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> validateIdAndGetBody(String entityId, String tenantId) throws ResponseException {
		// null id check
		if (entityId == null) {
			throw new ResponseException(ErrorType.BadRequestData, "empty entity id not allowed");
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

	@SuppressWarnings("unchecked")
	public boolean deleteEntry(ArrayListMultimap<String, String> headers, String entityId, BatchInfo batchInfo)
			throws ResponseException, Exception {
		logger.trace("deleteEntity() :: started");
		if (entityId == null) {
			throw new ResponseException(ErrorType.BadRequestData, "empty entity id not allowed");
		}
		String tenantId = HttpUtils.getInternalTenant(headers);
		Map<String, Object> oldEntity = (Map<String, Object>) JsonUtils
				.fromString(entityInfoDAO.getEntity(entityId, tenantId));

		EntityRequest request = new DeleteEntityRequest(entityId, headers);
		if (directDB) {
			pushToDB(request);
		}
		request.setRequestPayload(oldEntity);
		request.setFinalPayload(oldEntity);
		request.setBatchInfo(batchInfo);
		sendToKafka(request);
		logger.trace("deleteEntity() :: completed");
		return true;
	}

	public boolean deleteEntry(ArrayListMultimap<String, String> headers, String entityId)
			throws ResponseException, Exception {
		return deleteEntry(headers, entityId, new BatchInfo(-1, -1));
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
		// publish merged message
		// check if anything is changed.
		if (!request.getUpdateResult().getUpdated().isEmpty()) {
			handleRequest(request);
		}
		logger.trace("partialUpdateEntity() :: completed");
		return request.getUpdateResult();
	}

	public ResponseEntity<String> patchtoEndpoint(String entityId, ArrayListMultimap<String, String> headers,
			String payload, String attrId) throws ResponseException, Exception {
		String tenantid = HttpUtils.getInternalTenant(headers);
		String endpoint = entityInfoDAO.getEndpoint(entityId, tenantid);

		ResponseEntity<String> response = null;
		if (endpoint != null) {
			HttpHeaders header = new HttpHeaders();
			header.set(NGSIConstants.TENANT_HEADER, tenantid);
			header.set(AppConstants.CONTENT_TYPE, AppConstants.NGB_APPLICATION_JSON);

			HttpComponentsClientHttpRequestFactory requestFactory = new HttpComponentsClientHttpRequestFactory();
			RestTemplate restTemplate = new RestTemplate(requestFactory);

			logger.debug("url " + endpoint + "/ngsi-ld/v1/entities/" + entityId + "attrs" + attrId);
			HttpEntity<String> httpEntity = new HttpEntity<>(payload, header);
			String patchuri = endpoint + "/ngsi-ld/v1/entities/" + entityId + "/attrs/" + attrId;

			response = restTemplate.exchange(patchuri, HttpMethod.PATCH, httpEntity, String.class);
		}
		return response;
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
		handleRequest(request);
		logger.trace("deleteAttribute() :: completed");
		return true;
	}

	private void handleRequest(EntityRequest request) throws ResponseException {
		if (directDB) {
			pushToDB(request);
		}
		sendToKafka(request);
	}

	@Override
	public void sendFail(BatchInfo batchInfo) {
		BaseRequest request = new BaseRequest();
		request.setRequestType(AppConstants.BATCH_ERROR_REQUEST);
		request.setBatchInfo(batchInfo);
		request.setId("" + batchInfo.getBatchId());
		sendToKafka(request);

	}

}
