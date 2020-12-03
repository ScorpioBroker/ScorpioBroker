package eu.neclab.ngsildbroker.entityhandler.services;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.filosganga.geogson.model.Geometry;
import com.google.gson.JsonParseException;
import com.netflix.discovery.EurekaClient;
import eu.neclab.ngsildbroker.commons.constants.DBConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.AppendResult;
import eu.neclab.ngsildbroker.commons.datatypes.BatchFailure;
import eu.neclab.ngsildbroker.commons.datatypes.BatchResult;
import eu.neclab.ngsildbroker.commons.datatypes.CSourceRegistration;
import eu.neclab.ngsildbroker.commons.datatypes.Entity;
import eu.neclab.ngsildbroker.commons.datatypes.EntityDetails;
import eu.neclab.ngsildbroker.commons.datatypes.EntityInfo;
import eu.neclab.ngsildbroker.commons.datatypes.GeoProperty;
import eu.neclab.ngsildbroker.commons.datatypes.Information;
import eu.neclab.ngsildbroker.commons.datatypes.Property;
import eu.neclab.ngsildbroker.commons.datatypes.Relationship;
import eu.neclab.ngsildbroker.commons.datatypes.RestResponse;
import eu.neclab.ngsildbroker.commons.datatypes.TimeInterval;
import eu.neclab.ngsildbroker.commons.datatypes.UpdateResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.KafkaWriteException;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.ldcontext.ContextResolverBasic;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaOps;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;
import eu.neclab.ngsildbroker.entityhandler.config.EntityProducerChannel;
import eu.neclab.ngsildbroker.entityhandler.config.EntityTopicMap;
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

	final private String regexNgsildAttributeTypes = new String(NGSIConstants.NGSI_LD_PROPERTY + "|"
			+ NGSIConstants.NGSI_LD_RELATIONSHIP + "|" + NGSIConstants.NGSI_LD_GEOPROPERTY);

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
	 * @return RestResponse
	 * @throws KafkaWriteException,Exception
	 * @throws ResponseException
	 */
	public String createMessage(String payload) throws ResponseException, Exception {
		// get message channel for ENTITY_CREATE topic.
		logger.debug("createMessage() :: started");
		// MessageChannel messageChannel = producerChannels.createWriteChannel();
		JsonNode json = SerializationTools.parseJson(objectMapper, payload);
		JsonNode idNode = json.get(NGSIConstants.JSON_LD_ID);
		JsonNode type = json.get(NGSIConstants.JSON_LD_TYPE);
		// null id and type check
		if (idNode == null || type == null) {
			throw new ResponseException(ErrorType.BadRequestData);
		}
		String id = idNode.asText();
		logger.debug("entity id " + id);
		// check in-memory hashmap for id
		synchronized (this.entityIds) {
			if (this.entityIds.contains(id)) {
				throw new ResponseException(ErrorType.AlreadyExists);
			}
			this.entityIds.add(id);
		}
		String now = SerializationTools.formatter.format(Instant.now());
		setTemporalProperties(json, now, now, false);
		payload = objectMapper.writeValueAsString(json);
		String withSysAttrs = payload;
		// new Thread() {
		// public void run() {
		removeTemporalProperties(json); // remove createdAt/modifiedAt fields informed by the user
		String entityWithoutSysAttrs;
		try {
			entityWithoutSysAttrs = objectMapper.writeValueAsString(json);
			pushToDB(id, withSysAttrs, entityWithoutSysAttrs, objectMapper.writeValueAsString(getKeyValueEntity(json)));
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			logger.error(e);
		}

		// };
		// }.start();
		new Thread() {
			public void run() {

				try {
					registerContext(id.getBytes(NGSIConstants.ENCODE_FORMAT),
							withSysAttrs.getBytes(NGSIConstants.ENCODE_FORMAT));
					operations.pushToKafka(producerChannels.createWriteChannel(),
							id.getBytes(NGSIConstants.ENCODE_FORMAT),
							withSysAttrs.getBytes(NGSIConstants.ENCODE_FORMAT));
				} catch (UnsupportedEncodingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (URISyntaxException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ResponseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				// TODO use or remove ... why is the check below commented

			};
		}.start();

		/*
		 * // write to ENTITY topic after ENTITY_CREATE success.
		 * operations.pushToKafka(this.producerChannels.entityWriteChannel(),
		 * id.asText().getBytes(NGSIConstants.ENCODE_FORMAT),
		 * payload.getBytes(NGSIConstants.ENCODE_FORMAT));
		 * 
		 * // write to ENTITY_WITHOUT_SYSATTRS topic
		 * operations.pushToKafka(this.producerChannels.
		 * entityWithoutSysAttrsWriteChannel(),
		 * id.asText().getBytes(NGSIConstants.ENCODE_FORMAT),
		 * entityWithoutSysAttrs.getBytes(NGSIConstants.ENCODE_FORMAT)); // write to
		 * KVENTITY topic
		 * operations.pushToKafka(this.producerChannels.kvEntityWriteChannel(),
		 * id.asText().getBytes(NGSIConstants.ENCODE_FORMAT),
		 * objectMapper.writeValueAsBytes(getKeyValueEntity(json)));
		 */

		logger.debug("createMessage() :: completed");
		return id;
	}

	private void pushToDB(String key, String payload, String withoutSysAttrs, String kv) {
		boolean success = false;
		while (!success) {
			try {
				logger.debug("Received message: " + payload);
				logger.trace("Writing data...");
				if (storageWriterDao != null && storageWriterDao.storeEntity(key, payload, withoutSysAttrs, kv)) {

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
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
		}
		// removeId on failure

	}

	// public String createMessageTest(String payload) throws ResponseException {
	// JsonNode json = SerializationTools.parseJson(objectMapper, payload);
	// JsonNode id = json.get(NGSIConstants.QUERY_PARAMETER_ID);
	// operations.pushToKafka(this.producerChannels.entityWriteChannel(),
	// id.asText().getBytes(), payload.getBytes());
	// return id.asText();
	// }

	public JsonNode getKeyValueEntity(JsonNode json) {
		ObjectNode kvJsonObject = objectMapper.createObjectNode();
		Iterator<Map.Entry<String, JsonNode>> iter = json.fields();
		while (iter.hasNext()) {
			Map.Entry<String, JsonNode> entry = iter.next();
			if (entry.getKey().equals(NGSIConstants.JSON_LD_ID) || entry.getKey().equals(NGSIConstants.JSON_LD_TYPE)) {
				kvJsonObject.set(entry.getKey(), entry.getValue());
			} else if (entry.getValue().isArray()) {
				ArrayNode values = objectMapper.createArrayNode();
				Iterator<JsonNode> it = entry.getValue().elements();
				while (it.hasNext()) {
					ObjectNode attrObj = (ObjectNode) it.next();
					if (attrObj.has(NGSIConstants.JSON_LD_VALUE)) { // common members like createdAt do not have
						// hasValue/hasObject
						values.add(entry.getValue());
					} else if (attrObj.has(NGSIConstants.NGSI_LD_HAS_VALUE)) {
						values.add(attrObj.get(NGSIConstants.NGSI_LD_HAS_VALUE));
					} else if (attrObj.has(NGSIConstants.NGSI_LD_HAS_OBJECT)
							&& attrObj.get(NGSIConstants.NGSI_LD_HAS_OBJECT).isArray()
							&& attrObj.get(NGSIConstants.NGSI_LD_HAS_OBJECT).get(0).has(NGSIConstants.JSON_LD_ID)) {
						values.add(attrObj.get(NGSIConstants.NGSI_LD_HAS_OBJECT).get(0).get(NGSIConstants.JSON_LD_ID));
					}
				}
				if (values.size() == 1) {
					kvJsonObject.set(entry.getKey(), values.get(0));
				} else {
					kvJsonObject.set(entry.getKey(), values);
				}

			}
		}
		return kvJsonObject;
	}

	private void setTemporalProperties(JsonNode jsonNode, String createdAt, String modifiedAt, boolean rootOnly) {
		if (!jsonNode.isObject()) {
			return;
		}
		ObjectNode objectNode = (ObjectNode) jsonNode;
		if (!createdAt.isEmpty()) {
			objectNode.remove(NGSIConstants.NGSI_LD_CREATED_AT);
			objectNode.putArray(NGSIConstants.NGSI_LD_CREATED_AT).addObject()
					.put(NGSIConstants.JSON_LD_TYPE, NGSIConstants.NGSI_LD_DATE_TIME)
					.put(NGSIConstants.JSON_LD_VALUE, createdAt);
		}
		if (!modifiedAt.isEmpty()) {
			objectNode.remove(NGSIConstants.NGSI_LD_MODIFIED_AT);
			objectNode.putArray(NGSIConstants.NGSI_LD_MODIFIED_AT).addObject()
					.put(NGSIConstants.JSON_LD_TYPE, NGSIConstants.NGSI_LD_DATE_TIME)
					.put(NGSIConstants.JSON_LD_VALUE, modifiedAt);
		}
		if (rootOnly) {
			return;
		}

		Iterator<Map.Entry<String, JsonNode>> iter = objectNode.fields();
		while (iter.hasNext()) {
			Map.Entry<String, JsonNode> entry = iter.next();
			if (entry.getValue().isArray() && entry.getValue().has(0) && entry.getValue().get(0).isObject()) {
				ObjectNode attrObj = (ObjectNode) entry.getValue().get(0);
				// add createdAt/modifiedAt only to properties, geoproperties and relationships
				if (attrObj.has(NGSIConstants.JSON_LD_TYPE) && attrObj.get(NGSIConstants.JSON_LD_TYPE).isArray()
						&& attrObj.get(NGSIConstants.JSON_LD_TYPE).has(0)
						&& attrObj.get(NGSIConstants.JSON_LD_TYPE).get(0).asText().matches(regexNgsildAttributeTypes)) {
					setTemporalProperties(attrObj, createdAt, modifiedAt, rootOnly);
				}
			}
		}
	}

	private void removeTemporalProperties(JsonNode jsonNode) {
		if (!jsonNode.isObject()) {
			return;
		}
		ObjectNode objectNode = (ObjectNode) jsonNode;
		objectNode.remove(NGSIConstants.NGSI_LD_CREATED_AT);
		objectNode.remove(NGSIConstants.NGSI_LD_MODIFIED_AT);

		String regexNgsildAttributeTypes = new String(NGSIConstants.NGSI_LD_PROPERTY + "|"
				+ NGSIConstants.NGSI_LD_RELATIONSHIP + "|" + NGSIConstants.NGSI_LD_GEOPROPERTY);
		Iterator<Map.Entry<String, JsonNode>> iter = objectNode.fields();
		while (iter.hasNext()) {
			Map.Entry<String, JsonNode> entry = iter.next();
			if (entry.getValue().isArray() && entry.getValue().has(0) && entry.getValue().get(0).isObject()) {
				ObjectNode attrObj = (ObjectNode) entry.getValue().get(0);
				// add createdAt/modifiedAt only to properties, geoproperties and relationships
				if (attrObj.has(NGSIConstants.JSON_LD_TYPE) && attrObj.get(NGSIConstants.JSON_LD_TYPE).isArray()
						&& attrObj.get(NGSIConstants.JSON_LD_TYPE).has(0)
						&& attrObj.get(NGSIConstants.JSON_LD_TYPE).get(0).asText().matches(regexNgsildAttributeTypes)) {
					removeTemporalProperties(attrObj);
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
	public UpdateResult updateMessage(String entityId, String payload) throws ResponseException, Exception {
		logger.trace("updateMessage() :: started");
		// get message channel for ENTITY_UPDATE topic
		MessageChannel messageChannel = producerChannels.updateWriteChannel();
		String entityBody = validateIdAndGetBody(entityId);
		/*
		 * String payloadResolved=contextResolver.applyContext(payload);
		 * System.out.println(payloadResolved); String
		 * original=contextResolver.applyContext(new String(originalJson),
		 * contextResolver.getContext(entityId));
		 * System.out.println("origial :: "+original);
		 */
		// update fields
		JsonNode updateNode = objectMapper.readTree(payload);
		UpdateResult updateResult = this.updateFields(entityBody, updateNode, null);
		// pubilsh merged message
		// & check if anything is changed.
		if (updateResult.getStatus()) {
			if (directDB) {
				String entityWithoutSysAttrs = new String(updateResult.getJsonWithoutSysAttrs());
				String withSysAttrs = new String(updateResult.getJson());
				try {
					pushToDB(entityId, withSysAttrs, entityWithoutSysAttrs,
							objectMapper.writeValueAsString(getKeyValueEntity(objectMapper.readTree(withSysAttrs))));
				} catch (JsonProcessingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
			new Thread() {
				public void run() {
					try {
						operations.pushToKafka(messageChannel, entityId.getBytes(NGSIConstants.ENCODE_FORMAT),
								updateResult.getAppendedJsonFields().toString().getBytes());
						updateContext(entityId.getBytes(NGSIConstants.ENCODE_FORMAT), updateResult.getJson());
					} catch (UnsupportedEncodingException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (ResponseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				};
			}.start();
		}
		logger.trace("updateMessage() :: completed");
		return updateResult;
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

	/**
	 * Method to append fields in existing Entity in system/kafka topic
	 * 
	 * @param entityId - id of entity to be appended
	 * @param payload  - jsonld message containing fileds to be appended
	 * @return AppendResult
	 * @throws ResponseException
	 * @throws IOException
	 */
	public AppendResult appendMessage(String entityId, String payload, String overwriteOption)
			throws ResponseException, Exception {
		logger.trace("appendMessage() :: started");
		// get message channel for ENTITY_APPEND topic
		MessageChannel messageChannel = producerChannels.appendWriteChannel();
		// payload validation
		if (entityId == null) {
			throw new ResponseException(ErrorType.BadRequestData);
		}
		// get entity details
		String entityBody = validateIdAndGetBody(entityId);
		AppendResult appendResult = this.appendFields(entityBody, objectMapper.readTree(payload), overwriteOption);
		// get entity from ENTITY topic.
		// pubilsh merged message
		// check if anything is changed
		if (appendResult.getStatus()) {
			if (directDB) {
				String entityWithoutSysAttrs = new String(appendResult.getJsonWithoutSysAttrs());
				String withSysAttrs = new String(appendResult.getJson());
				try {
					pushToDB(entityId, withSysAttrs, entityWithoutSysAttrs,
							objectMapper.writeValueAsString(getKeyValueEntity(objectMapper.readTree(withSysAttrs))));
				} catch (JsonProcessingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
			new Thread() {
				public void run() {
					try {
						operations.pushToKafka(messageChannel, entityId.getBytes(NGSIConstants.ENCODE_FORMAT),
								objectMapper.writeValueAsBytes(appendResult.getJsonToAppend()));
					} catch (UnsupportedEncodingException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (JsonProcessingException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (ResponseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				};
			}.start();

			/*
			 * // write to ENTITY topic after ENTITY_APPEND success.
			 * operations.pushToKafka(this.producerChannels.entityWriteChannel(),
			 * entityId.getBytes(NGSIConstants.ENCODE_FORMAT), finalJson); // write to
			 * ENTITY_WITHOUT_SYSATTRS topic operations.pushToKafka(this.producerChannels.
			 * entityWithoutSysAttrsWriteChannel(),
			 * entityId.getBytes(NGSIConstants.ENCODE_FORMAT),
			 * appendResult.getJsonWithoutSysAttrs());
			 * operations.pushToKafka(this.producerChannels.kvEntityWriteChannel(),
			 * entityId.getBytes(NGSIConstants.ENCODE_FORMAT),
			 * objectMapper.writeValueAsBytes(getKeyValueEntity(appendResult.getFinalNode())
			 * ));
			 */
		}
		logger.trace("appendMessage() :: completed");
		return appendResult;
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

	public UpdateResult partialUpdateEntity(String entityId, String attrId, String payload)
			throws ResponseException, Exception {
		logger.trace("partialUpdateEntity() :: started");
		// get message channel for ENTITY_APPEND topic
		MessageChannel messageChannel = producerChannels.updateWriteChannel();
		if (entityId == null) {
			throw new ResponseException(ErrorType.BadRequestData);
		}
		// get entity details
		String entityBody = validateIdAndGetBody(entityId);
		// JsonNode originalJsonNode = objectMapper.readTree(originalJson);

		UpdateResult updateResult = this.updateFields(entityBody, objectMapper.readTree(payload), attrId);
		// pubilsh merged message
		// check if anything is changed.
		if (updateResult.getStatus()) {
			if (directDB) {
				JsonNode json = updateResult.getFinalNode();
				String withSysAttrs = new String(updateResult.getJson());
				removeTemporalProperties(json); // remove createdAt/modifiedAt fields informed by the user
				String entityWithoutSysAttrs;
				try {
					entityWithoutSysAttrs = new String(updateResult.getJsonWithoutSysAttrs());
					pushToDB(entityId, withSysAttrs, entityWithoutSysAttrs,
							objectMapper.writeValueAsString(getKeyValueEntity(objectMapper.readTree(withSysAttrs))));
				} catch (JsonProcessingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
			new Thread() {
				public void run() {
					try {
						operations.pushToKafka(messageChannel, entityId.getBytes(NGSIConstants.ENCODE_FORMAT),
								objectMapper.writeValueAsBytes(updateResult.getFinalNode()));
						updateContext(entityId.getBytes(NGSIConstants.ENCODE_FORMAT), updateResult.getJson());
					} catch (UnsupportedEncodingException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (ResponseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (JsonProcessingException e) {
						e.printStackTrace();
					}
				};
			}.start();

		}
		logger.trace("partialUpdateEntity() :: completed");
		return updateResult;

	}

	public boolean deleteAttribute(String entityId, String attrId,String datasetId,String deleteAll) throws ResponseException, Exception {
		logger.trace("deleteAttribute() :: started");
		// get message channel for ENTITY_APPEND topic
		MessageChannel messageChannel = producerChannels.deleteWriteChannel();
		if (entityId == null) {
			throw new ResponseException(ErrorType.BadRequestData);
		}
		// get entity details from in-memory hashmap
		String entityBody = validateIdAndGetBody(entityId);

		JsonNode finalJson = this.deleteFields(entityBody, attrId, datasetId, deleteAll);
		String finalFullEntity = objectMapper.writeValueAsString(finalJson);
		removeTemporalProperties(finalJson);
		String entityWithoutSysAttrs = objectMapper.writeValueAsString(finalJson);
		String kvEntity = objectMapper.writeValueAsString(getKeyValueEntity(finalJson));
		pushToDB(entityId, finalFullEntity, entityWithoutSysAttrs, kvEntity);

		// pubilsh updated message
		new Thread() {
			public void run() {
				try {
					operations.pushToKafka(messageChannel, entityId.getBytes(NGSIConstants.ENCODE_FORMAT),
							finalFullEntity.getBytes());
				} catch (UnsupportedEncodingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ResponseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			};
		}.start();

		logger.trace("deleteAttribute() :: completed");
		return true;
	}

	/**
	 * Method to merge/update fields in original Entitiy
	 * @param originalJsonObject
	 * @param jsonToUpdate
	 * @param attrId
	 * @return
	 * @throws Exception
	 * @throws ResponseException
	 */
	public UpdateResult updateFields(String originalJsonObject, JsonNode jsonToUpdate, String attrId)
			throws Exception, ResponseException {
		logger.trace("updateFields() :: started");
		String now = SerializationTools.formatter.format(Instant.now());
		JsonNode resultJson = objectMapper.createObjectNode();
		UpdateResult updateResult = new UpdateResult(jsonToUpdate, resultJson);
		JsonNode node = objectMapper.readTree(originalJsonObject);
		ObjectNode objectNode = (ObjectNode) node;
		if (attrId != null) {
			if (objectNode.get(attrId) == null) {
				throw new ResponseException(ErrorType.NotFound, "Provided attribute is not present");
			}
			JsonNode originalNode = ((ArrayNode) objectNode.get(attrId)).get(0);
			if (((ObjectNode) originalNode).has(NGSIConstants.NGSI_LD_INSTANCE_ID)) {
				((ObjectNode) originalNode).remove(NGSIConstants.NGSI_LD_INSTANCE_ID);
			}
			JsonNode innerNode = ((ArrayNode) objectNode.get(attrId));
			ArrayNode myArray = (ArrayNode) innerNode;
			String availableDatasetId = null;
			for (int i = 0; i < myArray.size(); i++) {
				if (myArray.get(i).has(NGSIConstants.NGSI_LD_DATA_SET_ID)) {
					String payloadDatasetId = myArray.get(i).get(NGSIConstants.NGSI_LD_DATA_SET_ID).get(0)
							.get(NGSIConstants.JSON_LD_ID).asText();
					if (jsonToUpdate.has(NGSIConstants.NGSI_LD_DATA_SET_ID)) {
						String datasetId = jsonToUpdate.get(NGSIConstants.NGSI_LD_DATA_SET_ID).get(0)
								.get(NGSIConstants.JSON_LD_ID).asText();
						if (payloadDatasetId.equalsIgnoreCase(datasetId)) {
							availableDatasetId = "available";
							setFieldValue(jsonToUpdate.fieldNames(), ((ArrayNode) objectNode.get(attrId)), jsonToUpdate,
									updateResult, i);
						}
					} else {
						if (payloadDatasetId.equals(NGSIConstants.DEFAULT_DATA_SET_ID)) {
							setFieldValue(jsonToUpdate.fieldNames(), ((ArrayNode) objectNode.get(attrId)), jsonToUpdate,
									updateResult, i);
						}
					}
				} else {
					if (jsonToUpdate.has(NGSIConstants.NGSI_LD_DATA_SET_ID)) {
						((ObjectNode) innerNode.get(i)).putArray(NGSIConstants.NGSI_LD_DATA_SET_ID).addObject()
								.put(NGSIConstants.JSON_LD_ID, NGSIConstants.DEFAULT_DATA_SET_ID);
					} else {
						((ObjectNode) innerNode.get(i)).putArray(NGSIConstants.NGSI_LD_DATA_SET_ID).addObject()
								.put(NGSIConstants.JSON_LD_ID, NGSIConstants.DEFAULT_DATA_SET_ID);
						setFieldValue(jsonToUpdate.fieldNames(), ((ArrayNode) objectNode.get(attrId)), jsonToUpdate,
								updateResult, i);
					}
				}
			}
			if (jsonToUpdate.has(NGSIConstants.NGSI_LD_DATA_SET_ID)) {
				if ((availableDatasetId == null) || (availableDatasetId.isEmpty())) {
					throw new ResponseException(ErrorType.NotFound, "Provided datasetId is not present");
				}
			}
		} else {
			Iterator<String> it = jsonToUpdate.fieldNames();
			while (it.hasNext()) {
				String field = it.next();
				// TOP level updates of context id or type are ignored
				if (field.equalsIgnoreCase(NGSIConstants.JSON_LD_CONTEXT)
						|| field.equalsIgnoreCase(NGSIConstants.JSON_LD_ID)
						|| field.equalsIgnoreCase(NGSIConstants.JSON_LD_TYPE)) {
					continue;
				}
				logger.trace("field: " + field);
				if (node.has(field)) {
					JsonNode originalNode = ((ArrayNode) objectNode.get(field)).get(0);
					JsonNode attrNode = jsonToUpdate.get(field).get(0);
					String createdAt = now;

					// keep original createdAt value if present in the original json
					if ((originalNode instanceof ObjectNode)
							&& ((ObjectNode) originalNode).has(NGSIConstants.NGSI_LD_CREATED_AT)
							&& ((ObjectNode) originalNode).get(NGSIConstants.NGSI_LD_CREATED_AT).isArray()) {
						createdAt = ((ObjectNode) ((ObjectNode) originalNode).get(NGSIConstants.NGSI_LD_CREATED_AT)
								.get(0)).get(NGSIConstants.JSON_LD_VALUE).asText();
					}
					setTemporalProperties(attrNode, createdAt, now, true);

					// TODO check if this should ever happen. 5.6.4.4 says BadRequest if AttrId is
					// present ...
					objectNode.replace(field, jsonToUpdate.get(field));
					((ObjectNode) updateResult.getAppendedJsonFields()).set(field, jsonToUpdate.get(field));
					logger.trace("appended json fields: " + updateResult.getAppendedJsonFields().toString());
					updateResult.setStatus(true);
				} else {
					// throw new ResponseException(ErrorType.NotFound);
				}
			}
		}
		setTemporalProperties(node, "", now, true); // root only, modifiedAt only
		updateResult.setJson(node.toString().getBytes(NGSIConstants.ENCODE_FORMAT));
		updateResult.setFinalNode(node);
		removeTemporalProperties(node);
		updateResult.setJsonWithoutSysAttrs(node.toString().getBytes(NGSIConstants.ENCODE_FORMAT));
		logger.trace("updateFields() :: completed");
		return updateResult;
	}

	/**
	 * Method to merge/append fileds in original Entity
	 * 
	 * @param originalJsonObject
	 * @param jsonToUpdate
	 * @return AppendResult
	 * @throws IOException
	 */
	public AppendResult appendFields(String originalJsonObject, JsonNode jsonToAppend, String overwriteOption)
			throws Exception {
		logger.trace("appendFields() :: started");
		String now = SerializationTools.formatter.format(Instant.now());
		JsonNode resultJson = objectMapper.createObjectNode();
		AppendResult appendResult = new AppendResult(jsonToAppend, resultJson);
		JsonNode node = objectMapper.readTree(originalJsonObject);
		ObjectNode objectNode = (ObjectNode) node;
		Iterator<String> it = jsonToAppend.fieldNames();
		while (it.hasNext()) {
			String key = it.next();
			if (key.equalsIgnoreCase(NGSIConstants.JSON_LD_CONTEXT) || key.equalsIgnoreCase(NGSIConstants.JSON_LD_ID)) {
				continue;
			}
			// remove if passed attribute have null value.
			if (jsonToAppend.get(key).isNull()) {
				objectNode.remove(key);
				((ObjectNode) appendResult.getAppendedJsonFields()).set(key, jsonToAppend.get(key));
				appendResult.setStatus(true);
				continue;
			}
			// validation append payload attribute
			/*
			 * if (!Validator.isValidAttribute(jsonToAppend.get(key))) { ((ObjectNode)
			 * appendResult.getAppendedJsonFields()).set(key, jsonToAppend.get(key));
			 * appendResult.setStatus(true); continue; }
			 */

			if ((objectNode.has(key) && !appendOverwriteFlag.equalsIgnoreCase(overwriteOption))
					|| !objectNode.has(key)) {
				if (jsonToAppend.get(key).isArray() && jsonToAppend.get(key).has(0)) {
					// TODO: should we keep the createdAt value if attribute already exists?
					// (overwrite operation) => if (objectNode.has(key)) ...
					JsonNode attrNode = jsonToAppend.get(key).get(0);
					setTemporalProperties(attrNode, now, now, true);
				}
				objectNode.replace(key, jsonToAppend.get(key));
				((ObjectNode) appendResult.getAppendedJsonFields()).set(key, jsonToAppend.get(key));
				appendResult.setStatus(true);
			}
		}
		setTemporalProperties(node, "", now, true); // root only, modifiedAt only
		appendResult.setJson(node.toString().getBytes(NGSIConstants.ENCODE_FORMAT));

		removeTemporalProperties(node);
		appendResult.setJsonWithoutSysAttrs(node.toString().getBytes(NGSIConstants.ENCODE_FORMAT));
		appendResult.setFinalNode(node);
		logger.trace("appendFields() :: completed");
		return appendResult;
	}

	/**
	 * Method to delete attributes from original Entity
	 * 
	 * @param originalJsonObject
	 * @param attrId
	 * @return
	 * @throws IOException
	 * @throws ResponseException
	 */
	public JsonNode deleteFields(String originalJsonObject, String attrId, String datasetId, String deleteAll) throws Exception, ResponseException {
		logger.trace("deleteFields() :: started");
		JsonNode node = objectMapper.readTree(originalJsonObject);
		ObjectNode objectNode = (ObjectNode) node;
		JsonNode innerNode = ((ArrayNode) objectNode.get(attrId));
		ArrayNode myArray = (ArrayNode) innerNode;
		String availableDatasetId = null;
		if (objectNode.has(attrId)) {
			//below condition remove the existing datasetId
			if (datasetId != null && !datasetId.isEmpty()) {
				for (int i = 0; i < myArray.size(); i++) {
					if (myArray.get(i).has(NGSIConstants.NGSI_LD_DATA_SET_ID)) {
						String payloadDatasetId = myArray.get(i).get(NGSIConstants.NGSI_LD_DATA_SET_ID).get(0)
								.get(NGSIConstants.JSON_LD_ID).asText();
						if (payloadDatasetId.equals(datasetId)) {
							availableDatasetId = "available";
							myArray.remove(i);
						}
					}
				}
				if ((availableDatasetId == null) || (availableDatasetId.isEmpty())) {
					throw new ResponseException(ErrorType.NotFound, "Provided datasetId is not present");
				}
              // below condition remove all the datasetId 
			} else if (deleteAll != null && !deleteAll.isEmpty()) {
				if (deleteAll.equals("true")) {
					if (objectNode.has(attrId)) {
						objectNode.remove(attrId);
					} else {
						throw new ResponseException(ErrorType.NotFound);
					}
				} else {
					throw new ResponseException(ErrorType.InvalidRequest, "request is not valid");
				}
			} else {
				// below condition remove the default datasetId
				for (int i = 0; i < myArray.size(); i++) {
					if (myArray.get(i).has(NGSIConstants.NGSI_LD_DATA_SET_ID)) {
						String payloadDatasetId = myArray.get(i).get(NGSIConstants.NGSI_LD_DATA_SET_ID).get(0)
								.get(NGSIConstants.JSON_LD_ID).asText();
						if (payloadDatasetId.equals(NGSIConstants.DEFAULT_DATA_SET_ID)) {
							availableDatasetId = "available";
							myArray.remove(i);
						}
					} else {
						availableDatasetId = "NotAvailable";
						myArray.remove(i);
					}
				}
				if ((availableDatasetId == null) || (availableDatasetId.isEmpty())) {
					throw new ResponseException(ErrorType.NotFound, "Default attribute instance is not present");
				}
			}
		} else {
			throw new ResponseException(ErrorType.NotFound, "Attribute is not present");
		}
		logger.trace("deleteFields() :: completed");
		return objectNode;
	}

	public boolean registerContext(byte[] id, byte[] payload)
			throws URISyntaxException, IOException, ResponseException {
		logger.trace("registerContext() :: started");
		MessageChannel messageChannel = producerChannels.contextRegistryWriteChannel();
		CSourceRegistration contextRegistryPayload = this.getCSourceRegistrationFromJson(payload);
		this.operations.pushToKafka(messageChannel, id, DataSerializer.toJson(contextRegistryPayload).getBytes());
		logger.trace("registerContext() :: completed");
		return true;
	}

	private void updateContext(byte[] id, byte[] payload) throws ResponseException {
		logger.trace("updateContext() :: started");
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

	public BatchResult createMultipleMessage(String payload) throws ResponseException {

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
					result.addSuccess(createMessage(objectMapper.writeValueAsString(next)));
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

	public BatchResult updateMultipleMessage(String resolved) throws ResponseException {
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
					AppendResult updateResult = appendMessage(entityId, objectMapper.writeValueAsString(next), null);
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

	public BatchResult upsertMultipleMessage(String resolved) throws ResponseException {
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

					result.addSuccess(createMessage(entityString));

				} catch (Exception e) {

					RestResponse response;
					if (e instanceof ResponseException) {
						ResponseException responseException = ((ResponseException) e);
						if (responseException.getHttpStatus().equals(HttpStatus.CONFLICT)) {
							AppendResult updateResult;
							try {
								updateResult = appendMessage(entityId, entityString, null);

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

	/**
	 * this method use for update the value of jsonNode.
	 * @param it
	 * @param innerNode
	 * @param jsonToUpdate
	 * @param updateResult
	 * @param i
	 */
	private void setFieldValue(Iterator<String> it, JsonNode innerNode, JsonNode jsonToUpdate, UpdateResult updateResult,
			int i) {
		while (it.hasNext()) {
			String field = it.next();
			// TOP level updates of context id or type are ignored
			if (field.equalsIgnoreCase(NGSIConstants.JSON_LD_CONTEXT)
					|| field.equalsIgnoreCase(NGSIConstants.JSON_LD_ID)
					|| field.equalsIgnoreCase(NGSIConstants.JSON_LD_TYPE)) {
				continue;
			}
			logger.trace("field: " + field);
			// logger.trace("attrId: " + attrId);
			if (innerNode != null) {
				((ObjectNode) innerNode.get(i)).replace(field, jsonToUpdate.get(field));
				logger.trace("appended json fields (partial): " + updateResult.getAppendedJsonFields().toString());
				updateResult.setStatus(true);
			}
		}
	}
}
