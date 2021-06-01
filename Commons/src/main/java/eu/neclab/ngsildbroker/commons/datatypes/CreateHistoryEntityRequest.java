package eu.neclab.ngsildbroker.commons.datatypes;

import java.net.URI;
import java.util.Map;

import com.google.common.collect.ArrayListMultimap;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public class CreateHistoryEntityRequest extends HistoryEntityRequest {

	JsonParser parser = new JsonParser();
	private boolean fromEntity;
	private JsonObject jsonObject;
	private URI uriId;
	private int attributeCount;

	public boolean isFromEntity() {
		return fromEntity;
	}

	public void setFromEntity(boolean fromEntity) {
		this.fromEntity = fromEntity;
	}

	public URI getUriId() {
		return uriId;
	}

	public void setUriId(URI uriId) {
		this.uriId = uriId;
	}

	public int getAttributeCount() {
		return attributeCount;
	}

	public void setAttributeCount(int attributeCount) {
		this.attributeCount = attributeCount;
	}

	/**
	 * Serialization constructor
	 * @param entityRequest 
	 * @throws ResponseException 
	 */
	public CreateHistoryEntityRequest(EntityRequest entityRequest) throws ResponseException {
		this(entityRequest.getHeaders(), entityRequest.getWithSysAttrs(), true);
	}

	public CreateHistoryEntityRequest(ArrayListMultimap<String, String> headers, String payload, boolean fromEntity)
			throws ResponseException {
		super(headers, payload);
		this.fromEntity = fromEntity;
		try {
			createTemporalEntity(payload, fromEntity);
		} catch (ResponseException e) {
			throw e;
		} catch (Exception e) {
			throw new ResponseException(e.getMessage());
		}
		// super(AppConstants.OPERATION_CREATE_HISTORY_ENTITY, headers);
	}
	
	private void createTemporalEntity(String payload, boolean fromEntity) throws ResponseException, Exception {
		this.jsonObject = parser.parse(payload).getAsJsonObject();
		if (jsonObject.get(NGSIConstants.JSON_LD_ID) == null || jsonObject.get(NGSIConstants.JSON_LD_TYPE) == null) {
			throw new ResponseException(ErrorType.InvalidRequest, "id and type are required fields");
		}
		this.attributeCount = 0;
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

		this.id = jsonObject.get(NGSIConstants.JSON_LD_ID).getAsString();
		this.type = jsonObject.get(NGSIConstants.JSON_LD_TYPE).getAsJsonArray().get(0).getAsString();
		this.createdAt = jsonObject.get(NGSIConstants.NGSI_LD_CREATED_AT).getAsJsonArray().get(0).getAsJsonObject()
				.get(NGSIConstants.JSON_LD_VALUE).getAsString();
		this.modifiedAt = jsonObject.get(NGSIConstants.NGSI_LD_MODIFIED_AT).getAsJsonArray().get(0).getAsJsonObject()
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
			// Boolean createTemporalEntityIfNotExists = (attributeCount == 0); // if it's the first attribute, create the
																				// //
			// temporalentity record

			if (entry.getValue().isJsonArray()) {
				JsonArray valueArray = entry.getValue().getAsJsonArray();
				// TODO check if changes in the array are reflect in the object
				for (JsonElement jsonElement : valueArray) {
					jsonElement = setCommonTemporalProperties(jsonElement, now, fromEntity);
					storeEntry(id, type, createdAt, modifiedAt, attribId, jsonElement.toString(), false);
					// pushAttributeToKafka(id, type, createdAt, modifiedAt, attribId,
					// jsonElement.toString(), createTemporalEntityIfNotExists, false);
				}
			}
			attributeCount++;
		}
		// attributeCount++; //move out }if(attributeCount==0)

		// { // create empty temporalentity (no attributes) TemporalEntityStorageKey
		// tesk = new TemporalEntityStorageKey(id); tesk.setEntityType(type);
		// tesk.setEntityCreatedAt(createdAt); tesk.setEntityModifiedAt(modifiedAt);
		// String messageKey = DataSerializer.toJson(tesk); logger.debug(" message key "
		// + messageKey + " payload element: empty"); /*
		// kafkaOperations.pushToKafka(producerChannels.temporalEntityWriteChannel(),
		// messageKey.getBytes(), "".getBytes());

		// }logger.trace("temporal entity created "+id);
		this.uriId = new URI(AppConstants.HISTORY_URL + id);
	}

	
}
