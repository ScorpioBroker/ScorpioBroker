package eu.neclab.ngsildbroker.historymanager.service;

import java.net.URI;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.TemporalEntityStorageKey;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaOps;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;
import eu.neclab.ngsildbroker.historymanager.config.ProducerChannel;
import eu.neclab.ngsildbroker.historymanager.repository.HistoryDAO;

@Service
public class HistoryService {

	private final static Logger logger = LoggerFactory.getLogger(HistoryService.class);

	@Autowired
	KafkaOps kafkaOperations;
	@Autowired
	ParamsResolver paramsResolver;
	@Autowired
	HistoryDAO historyDAO;

//	public static final Gson GSON = DataSerializer.GSON;

	JsonParser parser = new JsonParser();

	private final ProducerChannel producerChannels;

	public HistoryService(ProducerChannel producerChannels) {
		this.producerChannels = producerChannels;

	}

	public URI createTemporalEntityFromEntity(String payload) throws ResponseException, Exception {
		return createTemporalEntity(payload, true);
	}

	public URI createTemporalEntityFromBinding(String payload) throws ResponseException, Exception {
		return createTemporalEntity(payload, false);
	}

	private URI createTemporalEntity(String payload, boolean fromEntity) throws ResponseException, Exception {
		logger.trace("creating temporal entity");
		final JsonObject jsonObject = parser.parse(payload).getAsJsonObject();
		System.out.println(jsonObject.toString());

		if (jsonObject.get(NGSIConstants.JSON_LD_ID) == null || jsonObject.get(NGSIConstants.JSON_LD_TYPE) == null) {
			throw new ResponseException(ErrorType.InvalidRequest, "id and type are required fields");
		}
		String now = SerializationTools.formatter.format(Instant.now());

		if (jsonObject.get(NGSIConstants.NGSI_LD_CREATED_AT) == null
				|| jsonObject.get(NGSIConstants.NGSI_LD_CREATED_AT) == null) {
			JsonArray temp = new JsonArray();
			JsonObject tempObj = new JsonObject();
			tempObj.addProperty(NGSIConstants.JSON_LD_TYPE, "DateTime");
			tempObj.addProperty(NGSIConstants.JSON_LD_VALUE, now);
			temp.add(tempObj);
			if (jsonObject.get(NGSIConstants.NGSI_LD_CREATED_AT) == null) {
				jsonObject.add(NGSIConstants.NGSI_LD_CREATED_AT, temp);
			}
			if (jsonObject.get(NGSIConstants.NGSI_LD_MODIFIED_AT) == null) {
				jsonObject.add(NGSIConstants.NGSI_LD_MODIFIED_AT, temp);
			}
		}

		String id = jsonObject.get(NGSIConstants.JSON_LD_ID).getAsString();
		String type = jsonObject.get(NGSIConstants.JSON_LD_TYPE).getAsJsonArray().get(0).getAsString();
		String createdAt = jsonObject.get(NGSIConstants.NGSI_LD_CREATED_AT).getAsJsonArray().get(0).getAsJsonObject()
				.get(NGSIConstants.JSON_LD_VALUE).getAsString();
		String modifiedAt = jsonObject.get(NGSIConstants.NGSI_LD_MODIFIED_AT).getAsJsonArray().get(0).getAsJsonObject()
				.get(NGSIConstants.JSON_LD_VALUE).getAsString();

		Integer attributeCount = 0;
		for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
			logger.debug("Key = " + entry.getKey() + " Value = " + entry.getValue());
			if (entry.getKey().equalsIgnoreCase(NGSIConstants.JSON_LD_ID)
					|| entry.getKey().equalsIgnoreCase(NGSIConstants.JSON_LD_TYPE)
					|| entry.getKey().equalsIgnoreCase(NGSIConstants.NGSI_LD_CREATED_AT)
					|| entry.getKey().equalsIgnoreCase(NGSIConstants.NGSI_LD_MODIFIED_AT)
					|| entry.getKey().equalsIgnoreCase(NGSIConstants.NGSI_LD_INSTANCE_ID)) {
				continue;
			}
			String attribId = entry.getKey();
			Boolean createTemporalEntityIfNotExists = (attributeCount == 0); // if it's the first attribute, create the
																				// temporalentity record

			if (entry.getValue().isJsonArray()) {
				JsonArray valueArray = entry.getValue().getAsJsonArray();
				for (JsonElement jsonElement : valueArray) {
					jsonElement = setCommonTemporalProperties(jsonElement, now, fromEntity);
					pushAttributeToKafka(id, type, createdAt, modifiedAt, attribId, jsonElement.toString(),
							createTemporalEntityIfNotExists, false);
				}
			}
			attributeCount++;
		}
		if (attributeCount == 0) { // create empty temporalentity (no attributes)
			TemporalEntityStorageKey tesk = new TemporalEntityStorageKey(id);
			tesk.setEntityType(type);
			tesk.setEntityCreatedAt(createdAt);
			tesk.setEntityModifiedAt(modifiedAt);
			String messageKey = DataSerializer.toJson(tesk);
			logger.debug(" message key " + messageKey + " payload element: empty");
			kafkaOperations.pushToKafka(producerChannels.temporalEntityWriteChannel(), messageKey.getBytes(),
					"".getBytes());
		}
		logger.trace("temporal entity created " + id);
		return new URI(AppConstants.HISTORY_URL + id);
	}

	private JsonElement setCommonTemporalProperties(JsonElement jsonElement, String date, boolean fromEntity) {
		String valueCreatedAt;
		if (fromEntity) {
			// reuse modifiedAt field from Attribute in Entity, if exists
			if (jsonElement.getAsJsonObject().has(NGSIConstants.NGSI_LD_MODIFIED_AT)
					&& jsonElement.getAsJsonObject().get(NGSIConstants.NGSI_LD_MODIFIED_AT).isJsonArray()
					&& jsonElement.getAsJsonObject().get(NGSIConstants.NGSI_LD_MODIFIED_AT).getAsJsonArray()
							.get(0) != null
					&& jsonElement.getAsJsonObject().get(NGSIConstants.NGSI_LD_MODIFIED_AT).getAsJsonArray().get(0)
							.isJsonObject()
					&& jsonElement.getAsJsonObject().get(NGSIConstants.NGSI_LD_MODIFIED_AT).getAsJsonArray().get(0)
							.getAsJsonObject().has(NGSIConstants.JSON_LD_VALUE)) {
				valueCreatedAt = jsonElement.getAsJsonObject().get(NGSIConstants.NGSI_LD_MODIFIED_AT).getAsJsonArray()
						.get(0).getAsJsonObject().get(NGSIConstants.JSON_LD_VALUE).getAsString();
			} else {
				valueCreatedAt = date;
			}
		} else {
			valueCreatedAt = date;
		}
		// append/overwrite temporal fields. as we are creating new instances,
		// modifiedAt and createdAt are the same
		jsonElement = setTemporalProperty(jsonElement, NGSIConstants.NGSI_LD_CREATED_AT, valueCreatedAt);
		jsonElement = setTemporalProperty(jsonElement, NGSIConstants.NGSI_LD_MODIFIED_AT, valueCreatedAt);
		// system generated instance id
		UUID uuid = UUID.randomUUID();
		String instanceid = "urn" + ":" + "ngsi-ld" + ":" + uuid;
		jsonElement = setTemporalPropertyinstanceId(jsonElement, NGSIConstants.NGSI_LD_INSTANCE_ID, instanceid);
		return jsonElement;
	}

	private JsonElement setTemporalProperty(JsonElement jsonElement, String propertyName, String value) {
		JsonObject objAttribute = jsonElement.getAsJsonObject();
		objAttribute.remove(propertyName);
		JsonObject obj = new JsonObject();
		obj.addProperty(NGSIConstants.JSON_LD_TYPE, NGSIConstants.NGSI_LD_DATE_TIME);
		obj.addProperty(NGSIConstants.JSON_LD_VALUE, value);
		JsonArray arr = new JsonArray();
		arr.add(obj);
		objAttribute.add(propertyName, arr);
		return objAttribute;
	}

	// system generated instance id
	private JsonElement setTemporalPropertyinstanceId(JsonElement jsonElement, String propertyName, String value) {
		JsonObject objAttribute = jsonElement.getAsJsonObject();
		objAttribute.remove(propertyName);
		JsonObject obj = new JsonObject();
		obj.addProperty(NGSIConstants.JSON_LD_ID, value);
		JsonArray arr = new JsonArray();
		arr.add(obj);
		objAttribute.add(propertyName, arr);
		return objAttribute;
	}

	private void pushAttributeToKafka(String entityId, String entityType, String entityCreatedAt,
			String entityModifiedAt, String attributeId, String elementValue, Boolean createTemporalEntityIfNotExists,
			Boolean overwriteOp) throws ResponseException {
		String messageKey;
		TemporalEntityStorageKey tesk = new TemporalEntityStorageKey(entityId);
		if (createTemporalEntityIfNotExists != null && createTemporalEntityIfNotExists) {
			tesk.setEntityType(entityType);
			tesk.setEntityCreatedAt(entityCreatedAt);
			tesk.setEntityModifiedAt(entityModifiedAt);
			tesk.setAttributeId(attributeId);
			messageKey = DataSerializer.toJson(tesk);
		} else {
			tesk.setEntityModifiedAt(entityModifiedAt);
			tesk.setAttributeId(attributeId);
			tesk.setOverwriteOp(overwriteOp);
			messageKey = DataSerializer.toJson(tesk);
		}
		logger.debug(" message key " + messageKey + " payload element " + elementValue);
		kafkaOperations.pushToKafka(producerChannels.temporalEntityWriteChannel(), messageKey.getBytes(),
				elementValue.getBytes());
	}

	private void pushAttributeToKafka(String id, String entityModifiedAt, String attributeId, String elementValue)
			throws ResponseException {
		pushAttributeToKafka(id, null, null, entityModifiedAt, attributeId, elementValue, null, null);
	}

	public void delete(String entityId, String attributeId, String instanceId, List<Object> linkHeaders)
			throws ResponseException, Exception {
		logger.debug("deleting temporal entity with id : " + entityId + "and attributeId : " + attributeId);

		String resolvedAttrId = null;
		if (attributeId != null) {
			resolvedAttrId = paramsResolver.expandAttribute(attributeId, linkHeaders);
		}
		TemporalEntityStorageKey tesk = new TemporalEntityStorageKey(entityId);
		tesk.setAttributeId(resolvedAttrId);
		tesk.setInstanceId(instanceId);
		String messageKey = DataSerializer.toJson(tesk);
		logger.trace("message key created : " + messageKey);
		kafkaOperations.pushToKafka(producerChannels.temporalEntityWriteChannel(), messageKey.getBytes(),
				"null".getBytes());
		logger.trace("temporal entity (" + entityId + ") deleted");
	}

	// endpoint "/entities/{entityId}/attrs"
	public void addAttrib2TemporalEntity(String entityId, String payload) throws ResponseException, Exception {
		logger.trace("replace attribute in temporal entity");
		final JsonObject jsonObject = parser.parse(payload).getAsJsonObject();
		String now = SerializationTools.formatter.format(Instant.now());
		if (!historyDAO.entityExists(entityId)) {
			throw new ResponseException(ErrorType.NotFound, "You cannot create an attribute on a none existing entity");
		}
		for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
			logger.debug("Key = " + entry.getKey() + " Value = " + entry.getValue());
			if (entry.getKey().equalsIgnoreCase(NGSIConstants.JSON_LD_ID)
					|| entry.getKey().equalsIgnoreCase(NGSIConstants.JSON_LD_TYPE)
					|| entry.getKey().equalsIgnoreCase(NGSIConstants.NGSI_LD_CREATED_AT)
					|| entry.getKey().equalsIgnoreCase(NGSIConstants.NGSI_LD_MODIFIED_AT)) {
				continue;
			}

			String attribId = entry.getKey();
			if (entry.getValue().isJsonArray()) {
				JsonArray valueArray = entry.getValue().getAsJsonArray();
				Integer instanceCount = 0;
				for (JsonElement jsonElement : valueArray) {
					jsonElement = setCommonTemporalProperties(jsonElement, now, false);
					//
					Boolean overwriteOp = (instanceCount == 0); // if it's the first one, send the overwrite op to
																// delete current values
					pushAttributeToKafka(entityId, null, null, now, attribId, jsonElement.toString(), false,
							overwriteOp);
					instanceCount++;
				}
			}
		}
		logger.trace("attribute replaced in temporalentity " + entityId);
	}

	// for endpoint "entities/{entityId}/attrs/{attrId}/{instanceId}")
	public void modifyAttribInstanceTemporalEntity(String entityId, String payload, String attribId, String instanceId,
			List<Object> linkHeaders) throws ResponseException, Exception {

		String now = SerializationTools.formatter.format(Instant.now());

		String resolvedAttrId = null;
		if (attribId != null) {
			resolvedAttrId = paramsResolver.expandAttribute(attribId, linkHeaders);
		}

		// check if entityId + attribId + instanceid exists. if not, throw exception
		// ResourceNotFound
		QueryParams qp = new QueryParams();
		qp.setId(entityId);
		qp.setAttrs(resolvedAttrId);
		qp.setInstanceId(instanceId);
		List<String> entityList = historyDAO.query(qp);
		if (entityList.size() == 0) {
			throw new ResponseException(ErrorType.NotFound);
		}

		// get original createdAt
		String createdAt = now;
		String instanceIdAdd = null;
		JsonArray jsonArray = null;
		try {
			jsonArray = parser.parse(historyDAO.getListAsJsonArray(entityList)).getAsJsonArray();
			createdAt = jsonArray.get(0).getAsJsonObject().get(resolvedAttrId).getAsJsonArray().get(0).getAsJsonObject()
					.get(NGSIConstants.NGSI_LD_CREATED_AT).getAsJsonArray().get(0).getAsJsonObject()
					.get(NGSIConstants.JSON_LD_VALUE).getAsString();
		} catch (Exception e) {
			e.printStackTrace();
			logger.warn("original createdAt element not found, using current timestamp");
		}

		logger.debug("modify attribute instance in temporal entity " + entityId + " - " + resolvedAttrId + " - "
				+ createdAt);

		final JsonObject jsonObject = parser.parse(payload).getAsJsonObject();

		for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
			logger.debug("Key = " + entry.getKey() + " Value = " + entry.getValue());
			if (entry.getKey().equalsIgnoreCase(NGSIConstants.JSON_LD_ID)
					|| entry.getKey().equalsIgnoreCase(NGSIConstants.JSON_LD_TYPE)
					|| entry.getKey().equalsIgnoreCase(NGSIConstants.NGSI_LD_CREATED_AT)
					|| entry.getKey().equalsIgnoreCase(NGSIConstants.NGSI_LD_MODIFIED_AT)) {
				continue;
			}
			String attribIdPayload = entry.getKey();
			if (!attribIdPayload.equals(resolvedAttrId)) {
				throw new ResponseException(ErrorType.InvalidRequest,
						"attribute id in payload and in URL must be the same: " + attribIdPayload + " (payload) / "
								+ resolvedAttrId + " (URL)");
			}

			if (entry.getValue().isJsonArray()) {
				JsonArray valueArray = entry.getValue().getAsJsonArray();
				for (JsonElement jsonElement : valueArray) {
					if (jsonElement.getAsJsonObject().get(NGSIConstants.NGSI_LD_INSTANCE_ID) != null) {
						if (!jsonElement.getAsJsonObject().get(NGSIConstants.NGSI_LD_INSTANCE_ID).getAsJsonArray()
								.get(0).getAsJsonObject().get(NGSIConstants.JSON_LD_ID).getAsString()
								.equals(instanceId)) {
							throw new ResponseException(ErrorType.InvalidRequest,
									"instanceId in payload and in URL must be the same");
						}
					} else {
						instanceIdAdd = jsonArray.get(0).getAsJsonObject().get(resolvedAttrId).getAsJsonArray().get(0)
								.getAsJsonObject().get(NGSIConstants.NGSI_LD_INSTANCE_ID).getAsJsonArray().get(0)
								.getAsJsonObject().get(NGSIConstants.JSON_LD_ID).getAsString();
						jsonElement = setTemporalPropertyinstanceId(jsonElement, NGSIConstants.NGSI_LD_INSTANCE_ID,
								instanceIdAdd);
					}
					jsonElement = setTemporalProperty(jsonElement, NGSIConstants.NGSI_LD_CREATED_AT, createdAt);
					jsonElement = setTemporalProperty(jsonElement, NGSIConstants.NGSI_LD_MODIFIED_AT, now);
					pushAttributeToKafka(entityId, now, attribIdPayload, jsonElement.toString());
				}
			}
		}
		logger.trace("instance modified in temporalentity " + entityId);
	}

	/*
	
	 */
	@KafkaListener(topics = "${entity.create.topic}", groupId = "historyManagerCreate")
	public void handleEntityCreate(@Payload byte[] message, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key)
			throws Exception {
		logger.trace("Listener handleEntityCreate...");
		String payload = new String(message);
		logger.debug("Received message: " + payload);
		createTemporalEntityFromEntity(payload);
	}

	@KafkaListener(topics = "${entity.append.topic}", groupId = "historyManagerAppend")
	public void handleEntityAppend(@Payload byte[] message, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key)
			throws Exception {
		logger.trace("Listener handleEntityAppend...");

		logger.debug("Received key: " + key);
		String payload = new String(message);
		logger.debug("Received message: " + payload);

		String now = SerializationTools.formatter.format(Instant.now());

		final JsonObject jsonObject = parser.parse(payload).getAsJsonObject();
		for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
			logger.debug("Key = " + entry.getKey() + " Value = " + entry.getValue());
			if (entry.getKey().equalsIgnoreCase(NGSIConstants.JSON_LD_ID)
					|| entry.getKey().equalsIgnoreCase(NGSIConstants.JSON_LD_TYPE)
					|| entry.getKey().equalsIgnoreCase(NGSIConstants.NGSI_LD_CREATED_AT)
					|| entry.getKey().equalsIgnoreCase(NGSIConstants.NGSI_LD_MODIFIED_AT)) {
				continue;
			}
			String attribIdPayload = entry.getKey();

			if (entry.getValue().isJsonArray()) {
				JsonArray valueArray = entry.getValue().getAsJsonArray();
				for (JsonElement jsonElement : valueArray) {
					jsonElement = setCommonTemporalProperties(jsonElement, now, true);
					pushAttributeToKafka(key, now, attribIdPayload, jsonElement.toString());
				}
			}
		}

	}

	@KafkaListener(topics = "${entity.update.topic}", groupId = "historyManagerUpdate")
	public void handleEntityUpdate(@Payload byte[] message, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key)
			throws Exception {
		logger.trace("Listener handleEntityUpdate...");

		logger.debug("Received key: " + key);
		String payload = new String(message);
		logger.debug("Received message: " + payload);

		String now = SerializationTools.formatter.format(Instant.now());

		final JsonObject jsonObject = parser.parse(payload).getAsJsonObject();
		for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
			logger.debug("Key = " + entry.getKey() + " Value = " + entry.getValue());
			if (entry.getKey().equalsIgnoreCase(NGSIConstants.JSON_LD_ID)
					|| entry.getKey().equalsIgnoreCase(NGSIConstants.JSON_LD_TYPE)
					|| entry.getKey().equalsIgnoreCase(NGSIConstants.NGSI_LD_CREATED_AT)
					|| entry.getKey().equalsIgnoreCase(NGSIConstants.NGSI_LD_MODIFIED_AT)) {
				continue;
			}
			String attribIdPayload = entry.getKey();

			if (entry.getValue().isJsonArray()) {
				JsonArray valueArray = entry.getValue().getAsJsonArray();
				for (JsonElement jsonElement : valueArray) {
					jsonElement = setCommonTemporalProperties(jsonElement, now, true);
					pushAttributeToKafka(key, now, attribIdPayload, jsonElement.toString());
				}
			}
		}

	}

	@KafkaListener(topics = "${entity.delete.topic}", groupId = "historyManagerDelete")
	public void handleEntityDelete(@Payload byte[] message, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key)
			throws Exception {
		logger.trace("Listener handleEntityDelete...");

		logger.debug("Received key: " + key);
		String payload = new String(message);
		logger.debug("Received message: " + payload);
	}

}
