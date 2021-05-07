package eu.neclab.ngsildbroker.commons.datatypes;

import java.net.URI;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.ArrayListMultimap;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;

public class CreateHistoryEntityRequest extends EntityRequest{
	
	JsonParser parser = new JsonParser();
	
	/**
	 * Serialization constructor
	 */
	public CreateHistoryEntityRequest() {
	}
	
	public CreateHistoryEntityRequest(ArrayListMultimap<String, String> headers) {
		super(AppConstants.OPERATION_CREATE_HISTORY_ENTITY, headers);
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
					//pushAttributeToKafka(id, type, createdAt, modifiedAt, attribId, jsonElement.toString(),
					//		createTemporalEntityIfNotExists, false);
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
			//kafkaOperations.pushToKafka(producerChannels.temporalEntityWriteChannel(), messageKey.getBytes(),
			//		"".getBytes());
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


	
}
