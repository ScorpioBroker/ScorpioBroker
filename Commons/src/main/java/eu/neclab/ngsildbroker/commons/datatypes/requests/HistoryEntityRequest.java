package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.ArrayListMultimap;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.HistoryAttribInstance;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;

public class HistoryEntityRequest extends BaseRequest {

	private ArrayList<HistoryAttribInstance> attribs = new ArrayList<HistoryAttribInstance>();
	protected String type;
	protected String createdAt;
	protected String modifiedAt;
	protected String now;

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

	public HistoryEntityRequest(ArrayListMultimap<String, String> headers, Map<String, Object> resolved,
			String entityId, int requestType) throws ResponseException {
		super(headers, entityId, resolved, requestType);
		this.now = SerializationTools.formatter.format(Instant.now());

	}

	protected void storeEntry(String entityId, String entityType, String entityCreatedAt, String entityModifiedAt,
			String attributeId, String elementValue, String instanceId, Boolean overwriteOp) {
		attribs.add(new HistoryAttribInstance(entityId, entityType, entityCreatedAt, entityModifiedAt, attributeId,
				elementValue, instanceId, overwriteOp));
	}

	protected Map<String, Object> setCommonDateProperties(Map<String, Object> jsonElement, String now) {
		ArrayList<Object> temp = new ArrayList<Object>();
		HashMap<String, Object> tempObj = new HashMap<String, Object>();
		tempObj.put(NGSIConstants.JSON_LD_TYPE, NGSIConstants.NGSI_LD_DATE_TIME);
		tempObj.put(NGSIConstants.JSON_LD_VALUE, now);
		temp.add(tempObj);
		if (jsonElement.get(NGSIConstants.NGSI_LD_CREATED_AT) == null) {
			jsonElement.put(NGSIConstants.NGSI_LD_CREATED_AT, temp);
		}
		if (jsonElement.get(NGSIConstants.NGSI_LD_MODIFIED_AT) == null) {
			jsonElement.put(NGSIConstants.NGSI_LD_MODIFIED_AT, temp);
		}
		return jsonElement;
	}

	protected Map<String, Object> setCommonTemporalProperties(Map<String, Object> jsonElement, String now) {
		jsonElement = setCommonDateProperties(jsonElement, now);
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
}
