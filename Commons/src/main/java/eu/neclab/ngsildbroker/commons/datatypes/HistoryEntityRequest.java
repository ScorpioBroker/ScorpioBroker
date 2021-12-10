package eu.neclab.ngsildbroker.commons.datatypes;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.ArrayListMultimap;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;

public class HistoryEntityRequest extends BaseRequest {

	protected ArrayList<HistoryAttribInstance> attribs = new ArrayList<HistoryAttribInstance>();

	protected Map<String, Object> payload;

	protected String id;
	protected String type;
	protected String createdAt;
	protected String modifiedAt;
	protected String now;
	protected String instanceId;

	public Map<String, Object> getPayload() {
		return payload;
	}

	public void setPayload(Map<String, Object> payload) {
		this.payload = payload;
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

	public HistoryEntityRequest(ArrayListMultimap<String, String> headers, Map<String, Object> resolved)
			throws ResponseException {
		super(headers);
		this.payload = resolved;
		this.now = SerializationTools.formatter.format(Instant.now());

	}

	protected void storeEntry(String entityId, String entityType, String entityCreatedAt, String entityModifiedAt,
			String attributeId, String elementValue, Boolean overwriteOp) {
		attribs.add(new HistoryAttribInstance(entityId, entityType, entityCreatedAt, entityModifiedAt, attributeId,
				elementValue, overwriteOp));
	}

	protected Map<String, Object> setCommonTemporalProperties(Map<String, Object> jsonElement, String date,
			boolean fromEntity) {
		String valueCreatedAt;
		if (fromEntity) {
			// reuse modifiedAt field from Attribute in Entity, if exists
			if (jsonElement.containsKey(NGSIConstants.NGSI_LD_MODIFIED_AT)
					&& jsonElement.get(NGSIConstants.NGSI_LD_MODIFIED_AT) instanceof List
					&& !((List) jsonElement.get(NGSIConstants.NGSI_LD_MODIFIED_AT)).isEmpty()
					&& ((List) jsonElement.get(NGSIConstants.NGSI_LD_MODIFIED_AT)).get(0) instanceof Map
					&& ((List<Map<String, Object>>) jsonElement.get(NGSIConstants.NGSI_LD_MODIFIED_AT)).get(0)
							.containsKey(NGSIConstants.JSON_LD_VALUE)) {
				valueCreatedAt = (String) ((List<Map<String, Object>>) jsonElement
						.get(NGSIConstants.NGSI_LD_MODIFIED_AT)).get(0).get(NGSIConstants.JSON_LD_VALUE);
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

	protected Map<String, Object> setTemporalProperty(Map<String, Object> jsonElement, String propertyName,
			String value) {

		jsonElement.remove(propertyName);
		HashMap<String, Object> obj = new HashMap<String, Object>();
		obj.put(NGSIConstants.JSON_LD_TYPE, NGSIConstants.NGSI_LD_DATE_TIME);
		obj.put(NGSIConstants.JSON_LD_VALUE, value);
		ArrayList<Object> arr = new ArrayList<Object>();
		arr.add(obj);
		jsonElement.put(propertyName, arr);
		return jsonElement;
	}

	// system generated instance id
	protected Map<String, Object> setTemporalPropertyinstanceId(Map<String, Object> jsonElement, String propertyName,
			String value) {
		jsonElement.remove(propertyName);
		HashMap<String, Object> obj = new HashMap<String, Object>();
		obj.put(NGSIConstants.JSON_LD_ID, value);
		ArrayList<Object> arr = new ArrayList<Object>();
		arr.add(obj);
		jsonElement.put(propertyName, arr);
		return jsonElement;
	}

	public String getInstanceId() {
		return this.instanceId;
	}

	public void setInstanceId(String instanceId) {
		this.instanceId = instanceId;
	}

}
