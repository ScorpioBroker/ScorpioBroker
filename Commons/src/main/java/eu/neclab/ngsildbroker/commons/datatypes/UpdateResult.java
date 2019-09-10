package eu.neclab.ngsildbroker.commons.datatypes;

import com.fasterxml.jackson.databind.JsonNode;

public class UpdateResult {
	private JsonNode jsonToAppend;
	private JsonNode updatedJsonFields;// = new ArrayList<JsonNode>();
	private boolean status = false;
	private byte[] json;
	private byte[] jsonWithoutSysAttrs;

	public boolean getUpdateResult() {
		return jsonToAppend.size() == updatedJsonFields.size();
	}

	public UpdateResult(JsonNode jsonToAppend,JsonNode appendedJsonFields) {
		super();
		this.jsonToAppend = jsonToAppend;
		this.updatedJsonFields=appendedJsonFields;
	}

	public JsonNode getJsonToAppend() {
		return jsonToAppend;
	}

	public void setJsonToAppend(JsonNode jsonToAppend) {
		this.jsonToAppend = jsonToAppend;
	}

	public JsonNode getAppendedJsonFields() {
		return updatedJsonFields;
	}

	public void setAppendedJsonFields(JsonNode updatedJsonFields) {
		this.updatedJsonFields = updatedJsonFields;
	}

	public boolean getStatus() {
		return status;
	}

	public void setStatus(boolean status) {
		this.status = status;
	}

	public byte[] getJson() {
		return json;
	}

	public void setJson(byte[] json) {
		this.json = json;
	}
	
	public byte[] getJsonWithoutSysAttrs() {
		return jsonWithoutSysAttrs;
	}

	public void setJsonWithoutSysAttrs(byte[] jsonWithoutSysAttrs) {
		this.jsonWithoutSysAttrs = jsonWithoutSysAttrs;
	}
	
}
