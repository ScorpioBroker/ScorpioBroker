package eu.neclab.ngsildbroker.commons.datatypes;

import java.time.Instant;
import java.util.ArrayList;
import java.util.UUID;

import com.google.common.collect.ArrayListMultimap;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;

public class HistoryEntityRequest extends BaseRequest {

	protected ArrayList<HistoryAttribInstance> attribs = new ArrayList<HistoryAttribInstance>();
	JsonParser parser = new JsonParser();
	protected String payload;
	protected JsonObject jsonObject;
	protected String id;
	protected String type;
	protected String createdAt;
	protected String modifiedAt;
	protected String now;
	protected String instanceId;

	public String getPayload() {
		return payload;
	}

	public void setPayload(String payload) {
		this.payload = payload;
	}

	public JsonObject getJsonObject() {
		return jsonObject;
	}

	public void setJsonObject(JsonObject jsonObject) {
		this.jsonObject = jsonObject;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getCreatedAt() {
		return createdAt;
	}

	public ArrayList<HistoryAttribInstance> getAttribs() {
		return attribs;
	}

	public void setAttribs(ArrayList<HistoryAttribInstance> attribs) {
		this.attribs = attribs;
	}

	public void setCreatedAt(String createdAt) {
		this.createdAt = createdAt;
	}

	public String getModifiedAt() {
		return modifiedAt;
	}

	public void setModifiedAt(String modifiedAt) {
		this.modifiedAt = modifiedAt;
	}

	/**
	 * Serialization constructor
	 */
	public HistoryEntityRequest() {
	}

	public HistoryEntityRequest(ArrayListMultimap<String, String> headers, String payload) throws ResponseException {
		super(headers);
		this.payload = payload;
		this.now = SerializationTools.formatter.format(Instant.now());

	}

	protected void storeEntry(String entityId, String entityType, String entityCreatedAt, String entityModifiedAt,
			String attributeId, String elementValue, Boolean overwriteOp) {
		attribs.add(new HistoryAttribInstance(entityId, entityType, entityCreatedAt, entityModifiedAt, attributeId,
				elementValue, overwriteOp));
	}

	protected JsonElement setCommonTemporalProperties(JsonElement jsonElement, String date, boolean fromEntity) {
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

	protected JsonElement setTemporalProperty(JsonElement jsonElement, String propertyName, String value) {
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
	protected JsonElement setTemporalPropertyinstanceId(JsonElement jsonElement, String propertyName, String value) {
		JsonObject objAttribute = jsonElement.getAsJsonObject();
		objAttribute.remove(propertyName);
		JsonObject obj = new JsonObject();
		obj.addProperty(NGSIConstants.JSON_LD_ID, value);
		JsonArray arr = new JsonArray();
		arr.add(obj);
		objAttribute.add(propertyName, arr);
		return objAttribute;
	}

	public String getInstanceId() {
		return this.instanceId;
	}

	public void setInstanceId(String instanceId) {
		this.instanceId = instanceId;
	}
	

}
