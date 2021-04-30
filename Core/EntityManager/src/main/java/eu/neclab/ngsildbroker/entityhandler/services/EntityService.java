package eu.neclab.ngsildbroker.entityhandler.services;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLTransientConnectionException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.messaging.MessageChannel;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.github.filosganga.geogson.model.Geometry;
import com.google.common.collect.ArrayListMultimap;
import com.google.gson.JsonParseException;
import com.netflix.discovery.EurekaClient;

import eu.neclab.ngsildbroker.commons.constants.DBConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.AppendEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.AppendResult;
import eu.neclab.ngsildbroker.commons.datatypes.BatchFailure;
import eu.neclab.ngsildbroker.commons.datatypes.BatchResult;
import eu.neclab.ngsildbroker.commons.datatypes.CSourceRegistration;
import eu.neclab.ngsildbroker.commons.datatypes.CreateEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.DeleteAttributeRequest;
import eu.neclab.ngsildbroker.commons.datatypes.Entity;
import eu.neclab.ngsildbroker.commons.datatypes.EntityInfo;
import eu.neclab.ngsildbroker.commons.datatypes.EntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.GeoProperty;
import eu.neclab.ngsildbroker.commons.datatypes.Information;
import eu.neclab.ngsildbroker.commons.datatypes.Property;
import eu.neclab.ngsildbroker.commons.datatypes.Relationship;
import eu.neclab.ngsildbroker.commons.datatypes.RestResponse;
import eu.neclab.ngsildbroker.commons.datatypes.TimeInterval;
import eu.neclab.ngsildbroker.commons.datatypes.UpdateEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.UpdateResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.KafkaWriteException;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.ldcontext.ContextResolverBasic;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaOps;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import eu.neclab.ngsildbroker.entityhandler.config.EntityProducerChannel;
import eu.neclab.ngsildbroker.entityhandler.validationutil.IdValidationRule;
import eu.neclab.ngsildbroker.entityhandler.validationutil.PropertyValidatioRule;
import eu.neclab.ngsildbroker.entityhandler.validationutil.RelationshipValidationRule;
import eu.neclab.ngsildbroker.entityhandler.validationutil.TypeValidationRule;
import eu.neclab.ngsildbroker.entityhandler.validationutil.ValidationRules;

@Service
public class EntityService {

	@Value("${entity.topic}")
	String ENTITY_TOPIC;
	@Value("${entity.create.topic}")
	String ENTITY_CREATE_TOPIC;
	@Value("${entity.append.topic}")
	String ENTITY_APPEND_TOPIC;
	@Value("${entity.update.topic}")
	String ENTITY_UPDATE_TOPIC;
	@Value("${entity.delete.topic}")
	String ENTITY_DELETE_TOPIC;
	@Value("${bootstrap.servers}")
	String bootstrapServers;
	@Value("${append.overwrite}")
	String appendOverwriteFlag;
	@Value("${entity.index.topic}")
	String ENTITY_INDEX;

	@Value("${batchoperations.maxnumber.create:-1}")
	int maxCreateBatch;
	@Value("${batchoperations.maxnumber.update:-1}")
	int maxUpdateBatch;
	@Value("${batchoperations.maxnumber.upsert:-1}")
	int maxUpsertBatch;
	@Value("${batchoperations.maxnumber.delete:-1}")
	int maxDeleteBatch;

	boolean directDB = true;
	@Autowired
	@Qualifier("emstorage")
	StorageWriterDAO storageWriterDao;

	@Autowired
	EntityInfoDAO entityInfoDAO;

	@Autowired
	@Qualifier("emops")
	KafkaOps operations;

	@Autowired
	@Qualifier("emparamsres")
	ParamsResolver paramsResolver;

	public void setOperations(KafkaOps operations) {
		this.operations = operations;
	}

	ObjectMapper objectMapper;
	@Autowired
	private EurekaClient eurekaClient;
	@Autowired
	@Qualifier("emconRes")
	ContextResolverBasic contextResolver;
	/*
	 * @Autowired
	 * 
	 * @Qualifier("emtopicmap") EntityTopicMap entityTopicMap;
	 */

	private final EntityProducerChannel producerChannels;

	LocalDateTime start;
	LocalDateTime end;
	private Set<String> entityIds = new HashSet<String>();

	private final static Logger logger = LogManager.getLogger(EntityService.class);

	public EntityService(EntityProducerChannel producerChannels, ObjectMapper objectMapper) {
		this.producerChannels = producerChannels;
		this.objectMapper = objectMapper;
	}

	// @PostConstruct
	// private void setupContextResolver() {
	// this.contextResolver =
	// ContextResolverService.getInstance(producerChannels.atContextWriteChannel(),
	// operations);
	// }

	// construct in-memory
	@PostConstruct
	private void loadStoredEntitiesDetails() throws IOException {
		synchronized (this.entityIds) {
			this.entityIds = entityInfoDAO.getAllIds();
		}
		/*
		 * Map<String, EntityDetails> entities =
		 * this.operations.getAllEntitiesDetails();
		 * logger.trace("filling in-memory hashmap started:"); for (EntityDetails entity
		 * : entities.values()) { logger.trace("key :: " + entity.getKey());
		 * entityTopicMap.put(entity.getKey(), entity); }
		 */logger.trace("filling in-memory hashmap completed:");
	}

	/**
	 * Method to publish jsonld message to kafka topic
	 * 
	 * @param payload jsonld message
	 * @param headers
	 * @return RestResponse
	 * @throws KafkaWriteException,Exception
	 * @throws ResponseException
	 */
	public String createMessage(ArrayListMultimap<String, String> headers, String payload)
			throws ResponseException, Exception {
		// get message channel for ENTITY_CREATE topic.
		logger.debug("createMessage() :: started");
		// MessageChannel messageChannel = producerChannels.createWriteChannel();
		EntityRequest request = new CreateEntityRequest(payload, headers);
		synchronized (this.entityIds) {
			if (this.entityIds.contains(request.getId())) {
				throw new ResponseException(ErrorType.AlreadyExists);
			}
			this.entityIds.add(request.getId());
		}
		pushToDB(request);
		new Thread() {
			public void run() {
				try {
					registerContext(request);
					operations.pushToKafka(producerChannels.createWriteChannel(),
							request.getId().getBytes(NGSIConstants.ENCODE_FORMAT),
							DataSerializer.toJson(request).getBytes(NGSIConstants.ENCODE_FORMAT));
				} catch (URISyntaxException | IOException | ResponseException e) {
					logger.error(e);
				}
			};
		}.start();

		logger.debug("createMessage() :: completed");
		return request.getId();
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
	 * @param payload  - jsonld message containing fileds to be updated with updated
	 *                 values
	 * @return RestResponse
	 * @throws ResponseException
	 * @throws IOException
	 */
	public UpdateResult updateMessage(ArrayListMultimap<String, String> headers, String entityId, String payload)
			throws ResponseException, Exception {
		logger.trace("updateMessage() :: started");
		// get message channel for ENTITY_UPDATE topic
		MessageChannel messageChannel = producerChannels.updateWriteChannel();
		String entityBody = validateIdAndGetBody(entityId);
		UpdateEntityRequest request = new UpdateEntityRequest(headers, entityId, entityBody, payload, null);

		// update fields

		// pubilsh merged message
		// & check if anything is changed.
		if (request.getStatus()) {
			if (directDB) {
				pushToDB(request);
			}
			new Thread() {
				public void run() {
					try {
						operations.pushToKafka(messageChannel, entityId.getBytes(NGSIConstants.ENCODE_FORMAT),
								DataSerializer.toJson(request).getBytes(NGSIConstants.ENCODE_FORMAT));
						updateContext(request);
					} catch (URISyntaxException | IOException | ResponseException e) {
						logger.error(e);
					}
				};
			}.start();
		}
		logger.trace("updateMessage() :: completed");
		return request.getUpdateResult();
	}

	/**
	 * Method to append fields in existing Entity in system/kafka topic
	 * 
	 * @param entityId - id of entity to be appended
	 * @param payload  - jsonld message containing fileds to be appended
	 * @return AppendResult
	 * @throws ResponseException
	 * @throws IOException
	 */
	public AppendResult appendMessage(ArrayListMultimap<String, String> headers, String entityId, String payload,
			String overwriteOption) throws ResponseException, Exception {
		logger.trace("appendMessage() :: started");
		// get message channel for ENTITY_APPEND topic
		MessageChannel messageChannel = producerChannels.appendWriteChannel();
		// payload validation
		if (entityId == null) {
			throw new ResponseException(ErrorType.BadRequestData);
		}
		// get entity details
		String entityBody = validateIdAndGetBody(entityId);
		AppendEntityRequest request = new AppendEntityRequest(headers, entityId, entityBody, payload, overwriteOption,
				this.appendOverwriteFlag);
		// get entity from ENTITY topic.
		// pubilsh merged message
		// check if anything is changed
		if (request.getStatus()) {
			if (directDB) {
				pushToDB(request);
			}
			new Thread() {
				public void run() {
					try {
						operations.pushToKafka(messageChannel, entityId.getBytes(NGSIConstants.ENCODE_FORMAT),
								DataSerializer.toJson(request).getBytes());
					} catch (IOException | ResponseException e) {
						logger.error(e);
					}
				};
			}.start();
		}
		logger.trace("appendMessage() :: completed");
		return request.getAppendResult();
	}

	private String validateIdAndGetBody(String entityId) throws ResponseException {
		// null id check
		if (entityId == null) {
			throw new ResponseException(ErrorType.BadRequestData);
		}
		// get entity details from in-memory hashmap.
		synchronized (this.entityIds) {
			if (!this.entityIds.contains(entityId)) {
				throw new ResponseException(ErrorType.NotFound);
			}
		}
		String entityBody = null;
		if (directDB) {
			entityBody = this.entityInfoDAO.getEntity(entityId);
		}
		return entityBody;
	}

	public boolean deleteEntity(String entityId) throws ResponseException, Exception {
		logger.trace("deleteEntity() :: started");
		// get message channel for ENTITY_DELETE topic
		MessageChannel messageChannel = producerChannels.deleteWriteChannel();
		if (entityId == null) {
			throw new ResponseException(ErrorType.BadRequestData);
		}
		// get entity details from in-memory hashmap
		synchronized (this.entityIds) {
			if (!this.entityIds.remove(entityId)) {
				throw new ResponseException(ErrorType.NotFound);
			}
			if (directDB) {
				storageWriterDao.store(DBConstants.DBTABLE_ENTITY, DBConstants.DBCOLUMN_DATA, entityId, null);
			}
		}
		new Thread() {
			public void run() {
				try {
					operations.pushToKafka(messageChannel, entityId.getBytes(NGSIConstants.ENCODE_FORMAT),
							"{}".getBytes());
				} catch (UnsupportedEncodingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ResponseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			};
		}.start();
		/*
		 * EntityDetails entityDetails = entityTopicMap.get(entityId); if (entityDetails
		 * == null) { throw new ResponseException(ErrorType.NotFound); } // get entity
		 * from entity topic byte[] originalJson =
		 * this.operations.getMessage(this.ENTITY_TOPIC, entityId,
		 * entityDetails.getPartition(), entityDetails.getOffset()); // check whether
		 * exists. if (originalJson == null) { throw new
		 * ResponseException(ErrorType.NotFound); } // TODO use or remove ... why is the
		 * check below commented boolean result =
		 * this.operations.pushToKafka(messageChannel,
		 * entityId.getBytes(NGSIConstants.ENCODE_FORMAT), originalJson);
		 * 
		 * if (!result) { throw new ResponseException(ErrorType.KafkaWriteError); }
		 * 
		 * operations.pushToKafka(this.producerChannels.entityWriteChannel(),
		 * entityId.getBytes(NGSIConstants.ENCODE_FORMAT),
		 * "null".getBytes(NGSIConstants.ENCODE_FORMAT));
		 * operations.pushToKafka(this.producerChannels.
		 * entityWithoutSysAttrsWriteChannel(),
		 * entityId.getBytes(NGSIConstants.ENCODE_FORMAT),
		 * "null".getBytes(NGSIConstants.ENCODE_FORMAT));
		 * operations.pushToKafka(this.producerChannels.kvEntityWriteChannel(),
		 * entityId.getBytes(NGSIConstants.ENCODE_FORMAT),
		 * "null".getBytes(NGSIConstants.ENCODE_FORMAT));
		 * 
		 */ logger.trace("deleteEntity() :: completed");
		return true;
	}

	public UpdateResult partialUpdateEntity(ArrayListMultimap<String, String> headers, String entityId, String attrId,
			String payload) throws ResponseException, Exception {
		logger.trace("partialUpdateEntity() :: started");
		// get message channel for ENTITY_APPEND topic
		MessageChannel messageChannel = producerChannels.updateWriteChannel();
		if (entityId == null) {
			throw new ResponseException(ErrorType.BadRequestData);
		}
		// get entity details
		String entityBody = validateIdAndGetBody(entityId);
		// JsonNode originalJsonNode = objectMapper.readTree(originalJson);
		UpdateEntityRequest request = new UpdateEntityRequest(headers, entityId, entityBody, payload, attrId);
		// pubilsh merged message
		// check if anything is changed.
		if (request.getStatus()) {
			if (directDB) {
				pushToDB(request);
			}
			new Thread() {
				public void run() {
					try {
						operations.pushToKafka(messageChannel, entityId.getBytes(NGSIConstants.ENCODE_FORMAT),
								DataSerializer.toJson(request).getBytes());
						updateContext(request);
					} catch (URISyntaxException | IOException | ResponseException e) {
						logger.error(e);
					}
				};
			}.start();

		}
		logger.trace("partialUpdateEntity() :: completed");
		return request.getUpdateResult();
	}

	public boolean deleteAttribute(ArrayListMultimap<String, String> headers, String entityId, String attrId,
			String datasetId, String deleteAll) throws ResponseException, Exception {
		logger.trace("deleteAttribute() :: started");
		// get message channel for ENTITY_APPEND topic
		MessageChannel messageChannel = producerChannels.deleteWriteChannel();
		if (entityId == null) {
			throw new ResponseException(ErrorType.BadRequestData);
		}
		// get entity details from in-memory hashmap
		String entityBody = validateIdAndGetBody(entityId);
		DeleteAttributeRequest request = new DeleteAttributeRequest(headers, entityId, entityBody, attrId, datasetId,
				deleteAll);
		if (directDB) {
			pushToDB(request);
		}
		new Thread() {
			public void run() {
				try {
					operations.pushToKafka(messageChannel, entityId.getBytes(NGSIConstants.ENCODE_FORMAT),
							DataSerializer.toJson(request).getBytes());
					updateContext(request);
				} catch (URISyntaxException | IOException | ResponseException e) {
					logger.error(e);
				}
			};
		}.start();

		logger.trace("deleteAttribute() :: completed");
		return true;
	}

	public boolean registerContext(EntityRequest request) throws URISyntaxException, IOException, ResponseException {
		// TODO needs rework as well
		logger.trace("registerContext() :: started");
		MessageChannel messageChannel = producerChannels.contextRegistryWriteChannel();
		byte[] id = request.getId().getBytes(NGSIConstants.ENCODE_FORMAT);
		byte[] payload = request.getWithSysAttrs().getBytes(NGSIConstants.ENCODE_FORMAT);
		CSourceRegistration contextRegistryPayload = this.getCSourceRegistrationFromJson(payload);
		this.operations.pushToKafka(messageChannel, id, DataSerializer.toJson(contextRegistryPayload).getBytes());
		logger.trace("registerContext() :: completed");
		return true;
	}

	private void updateContext(EntityRequest request) throws URISyntaxException, IOException, ResponseException {
		// TODO needs rework as well
		logger.trace("updateContext() :: started");
		byte[] id = request.getId().getBytes(NGSIConstants.ENCODE_FORMAT);
		byte[] payload = request.getWithSysAttrs().getBytes(NGSIConstants.ENCODE_FORMAT);
		MessageChannel messageChannel = producerChannels.contextUpdateWriteChannel();
		this.operations.pushToKafka(messageChannel, id, payload);
		logger.trace("updateContext() :: completed");
	}

	private CSourceRegistration getCSourceRegistrationFromJson(byte[] payload) throws URISyntaxException, IOException {
		logger.trace("getCSourceRegistrationFromJson() :: started");
		CSourceRegistration csourceRegistration = new CSourceRegistration();
		List<Information> information = new ArrayList<Information>();
		Information info = new Information();
		List<EntityInfo> entities = info.getEntities();
		Entity entity = DataSerializer.getEntity(objectMapper.writeValueAsString(objectMapper.readTree(payload)));

		// Entity to CSourceRegistration conversion.
		csourceRegistration.setId(entity.getId());
		csourceRegistration.setEndpoint(MicroServiceUtils.getGatewayURL(eurekaClient));
		// location node
		GeoProperty geoLocationProperty = entity.getLocation();
		if (geoLocationProperty != null) {
			csourceRegistration.setLocation(getCoveringGeoValue(geoLocationProperty));
		}

		// Information node
		Set<String> propertiesList = entity.getProperties().stream().map(Property::getIdString)
				.collect(Collectors.toSet());

		Set<String> relationshipsList = entity.getRelationships().stream().map(Relationship::getIdString)
				.collect(Collectors.toSet());

		entities.add(new EntityInfo(entity.getId(), null, entity.getType()));

		info.setProperties(propertiesList);
		info.setRelationships(relationshipsList);
		information.add(info);
		csourceRegistration.setInformation(information);

		// location node.

		TimeInterval timestamp = new TimeInterval();
		timestamp.setStart(new Date().getTime());
		csourceRegistration.setTimestamp(timestamp);
		logger.trace("getCSourceRegistrationFromJson() :: completed");
		return csourceRegistration;
	}

	private Geometry<?> getCoveringGeoValue(GeoProperty geoLocationProperty) {
		// TODO should be done better to cover the actual area
		return geoLocationProperty.getEntries().values().iterator().next().getGeoValue();
	}

	public URI getResourceURL(String resource) throws URISyntaxException {
		logger.trace("getResourceURL() :: started");
		URI uri = MicroServiceUtils.getGatewayURL(eurekaClient);
		logger.trace("getResourceURL() :: completed");
		return new URI(uri.toString() + "/" + resource);
	}

	/*
	 * @KafkaListener(topics = "${entity.topic}", groupId = "entitymanager") public
	 * void updateTopicDetails(Message<byte[]> message) throws IOException {
	 * logger.trace("updateTopicDetails() :: started"); String key =
	 * operations.getMessageKey(message); int partitionId = (int)
	 * message.getHeaders().get(KafkaHeaders.RECEIVED_PARTITION_ID); long offset =
	 * (long) message.getHeaders().get(KafkaHeaders.OFFSET); JsonNode entityJsonBody
	 * = objectMapper.readTree(message.getPayload()); boolean isDeletedMsg =
	 * entityJsonBody.isNull(); if (isDeletedMsg) { entityTopicMap.remove(key); }
	 * else { entityTopicMap.put(key, new EntityDetails(key, partitionId, offset));
	 * } logger.trace("updateTopicDetails() :: completed"); }
	 */

	public void validateEntity(String payload, HttpServletRequest request) throws ResponseException {
		Entity entity;
		try {
			entity = DataSerializer.getEntity(payload);
		} catch (JsonParseException e) {
			throw new ResponseException(ErrorType.BadRequestData, e.getMessage());
		}
		List<ValidationRules> rules = new ArrayList<>();
		rules.add(new IdValidationRule());
		rules.add(new TypeValidationRule());
		rules.add(new PropertyValidatioRule());
		rules.add(new RelationshipValidationRule());

		for (ValidationRules rule : rules) {
			rule.validateEntity(entity, request);
		}
	}

	public BatchResult createMultipleMessage(ArrayListMultimap<String, String> headers, String payload)
			throws ResponseException {

		try {
			BatchResult result = new BatchResult();
			JsonNode myTree = objectMapper.readTree(payload);
			if (!myTree.isArray()) {
				throw new ResponseException(ErrorType.InvalidRequest,
						"This interface only supports arrays of entities");
			}
			ArrayNode myArray = (ArrayNode) myTree;
			if (maxCreateBatch != -1 && myArray.size() > maxCreateBatch) {
				throw new ResponseException(ErrorType.RequestEntityTooLarge,
						"Maximum allowed number of entities for this operation is " + maxCreateBatch);
			}
			Iterator<JsonNode> it = myArray.iterator();
			while (it.hasNext()) {
				JsonNode next = it.next();

				try {
					result.addSuccess(createMessage(headers, objectMapper.writeValueAsString(next)));
				} catch (Exception e) {

					String entityId = "NOT AVAILABLE";
					if (next.hasNonNull(NGSIConstants.JSON_LD_ID)) {
						entityId = next.get(NGSIConstants.JSON_LD_ID).asText();
					}
					RestResponse response;
					if (e instanceof ResponseException) {
						response = new RestResponse((ResponseException) e);
					} else {
						response = new RestResponse(ErrorType.InternalError, e.getLocalizedMessage());
					}

					result.addFail(new BatchFailure(entityId, response));
				}

			}
			return result;
		} catch (IOException e) {
			throw new ResponseException(ErrorType.BadRequestData, e.getMessage());
		}

	}

	public BatchResult deleteMultipleMessage(String payload) throws ResponseException {
		try {
			BatchResult result = new BatchResult();
			JsonNode myTree = objectMapper.readTree(payload);
			if (!myTree.isArray()) {
				throw new ResponseException(ErrorType.InvalidRequest,
						"This interface only supports arrays of entities");
			}
			ArrayNode myArray = (ArrayNode) myTree;
			if (maxDeleteBatch != -1 && myArray.size() > maxDeleteBatch) {
				throw new ResponseException(ErrorType.RequestEntityTooLarge,
						"Maximum allowed number of entities for this operation is " + maxDeleteBatch);
			}

			Iterator<JsonNode> it = myArray.iterator();
			while (it.hasNext()) {
				JsonNode next = it.next();
				String entityId = next.asText();
				try {
					if (deleteEntity(entityId)) {
						result.addSuccess(entityId);
					}
				} catch (Exception e) {
					RestResponse response;
					if (e instanceof ResponseException) {
						response = new RestResponse((ResponseException) e);
					} else {
						response = new RestResponse(ErrorType.InternalError, e.getLocalizedMessage());
					}

					result.addFail(new BatchFailure(entityId, response));
				}
			}
			return result;
		} catch (IOException e) {
			throw new ResponseException(ErrorType.BadRequestData, e.getMessage());
		}

	}

	public BatchResult updateMultipleMessage(ArrayListMultimap<String, String> headers, String resolved)
			throws ResponseException {
		try {
			BatchResult result = new BatchResult();
			JsonNode myTree = objectMapper.readTree(resolved);
			if (!myTree.isArray()) {
				throw new ResponseException(ErrorType.InvalidRequest,
						"This interface only supports arrays of entities");
			}
			ArrayNode myArray = (ArrayNode) myTree;
			if (maxUpdateBatch != -1 && myArray.size() > maxUpdateBatch) {
				throw new ResponseException(ErrorType.RequestEntityTooLarge,
						"Maximum allowed number of entities for this operation is " + maxUpdateBatch);
			}
			Iterator<JsonNode> it = myArray.iterator();
			while (it.hasNext()) {
				JsonNode next = it.next();
				String entityId = "NOT AVAILABLE";
				if (next.hasNonNull(NGSIConstants.JSON_LD_ID)) {
					entityId = next.get(NGSIConstants.JSON_LD_ID).asText();
				} else {
					result.addFail(new BatchFailure(entityId,
							new RestResponse(ErrorType.BadRequestData, "No Entity Id provided")));
					continue;
				}
				try {
					AppendResult updateResult = appendMessage(headers, entityId, objectMapper.writeValueAsString(next),
							null);
					if (updateResult.getStatus()) {
						result.addSuccess(entityId);
					} else {
						result.addFail(new BatchFailure(entityId, new RestResponse(ErrorType.MultiStatus,
								objectMapper.writeValueAsString(updateResult.getJsonToAppend()) + " was not added")));
					}

				} catch (Exception e) {

					RestResponse response;
					if (e instanceof ResponseException) {
						response = new RestResponse((ResponseException) e);
					} else {
						response = new RestResponse(ErrorType.InternalError, e.getLocalizedMessage());
					}

					result.addFail(new BatchFailure(entityId, response));
				}

			}
			return result;
		} catch (IOException e) {
			throw new ResponseException(ErrorType.BadRequestData, e.getMessage());
		}
	}

	public BatchResult upsertMultipleMessage(ArrayListMultimap<String, String> headers, String resolved)
			throws ResponseException {
		try {
			BatchResult result = new BatchResult();
			JsonNode myTree = objectMapper.readTree(resolved);
			if (!myTree.isArray()) {
				throw new ResponseException(ErrorType.InvalidRequest,
						"This interface only supports arrays of entities");
			}
			ArrayNode myArray = (ArrayNode) myTree;
			if (maxUpsertBatch != -1 && myArray.size() > maxUpsertBatch) {
				throw new ResponseException(ErrorType.RequestEntityTooLarge,
						"Maximum allowed number of entities for this operation is " + maxUpsertBatch);
			}
			Iterator<JsonNode> it = myArray.iterator();
			while (it.hasNext()) {
				JsonNode next = it.next();
				String entityId = "NOT AVAILABLE";
				if (next.hasNonNull(NGSIConstants.JSON_LD_ID)) {
					entityId = next.get(NGSIConstants.JSON_LD_ID).asText();
				} else {
					result.addFail(new BatchFailure(entityId,
							new RestResponse(ErrorType.BadRequestData, "No Entity Id provided")));
					continue;
				}
				String entityString = objectMapper.writeValueAsString(next);
				try {

					result.addSuccess(createMessage(headers, entityString));

				} catch (Exception e) {

					RestResponse response;
					if (e instanceof ResponseException) {
						ResponseException responseException = ((ResponseException) e);
						if (responseException.getHttpStatus().equals(HttpStatus.CONFLICT)) {
							AppendResult updateResult;
							try {
								updateResult = appendMessage(headers, entityId, entityString, null);

								if (updateResult.getStatus()) {
									result.addSuccess(entityId);
								} else {
									result.addFail(new BatchFailure(entityId,
											new RestResponse(ErrorType.MultiStatus,
													objectMapper.writeValueAsString(updateResult.getJsonToAppend())
															+ " was not added")));
								}
							} catch (Exception e1) {

								if (e1 instanceof ResponseException) {
									response = new RestResponse((ResponseException) e1);
								} else {
									response = new RestResponse(ErrorType.InternalError, e1.getLocalizedMessage());
								}

								result.addFail(new BatchFailure(entityId, response));
							}
						} else {
							response = new RestResponse((ResponseException) e);
							result.addFail(new BatchFailure(entityId, response));
						}

					} else {
						response = new RestResponse(ErrorType.InternalError, e.getLocalizedMessage());
						result.addFail(new BatchFailure(entityId, response));
					}

				}

			}
			return result;
		} catch (IOException e) {
			throw new ResponseException(ErrorType.BadRequestData, e.getMessage());
		}
	}

}
