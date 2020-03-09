package eu.neclab.ngsildbroker.commons.datatypes;

import com.fasterxml.jackson.databind.JsonNode;

public class AppendResult {
	
	private JsonNode jsonToAppend;
	private JsonNode appendedJsonFields;// = new ArrayList<JsonNode>();
	private JsonNode finalNode;
	private boolean status=false;
	private byte[] json;
	private byte[] jsonWithoutSysAttrs;

	public boolean getAppendResult() {
		return jsonToAppend.size()==appendedJsonFields.size();
	}
	
	public AppendResult(JsonNode jsonToAppend,JsonNode appendedJsonFields) {
		super();
		this.jsonToAppend = jsonToAppend;
		this.appendedJsonFields=appendedJsonFields;
	}

	public JsonNode getFinalNode() {
		return finalNode;
	}

	public void setFinalNode(JsonNode finalNode) {
		this.finalNode = finalNode;
	}

	public JsonNode getJsonToAppend() {
		return jsonToAppend;
	}

	public void setJsonToAppend(JsonNode jsonToAppend) {
		this.jsonToAppend = jsonToAppend;
	}

	public JsonNode getAppendedJsonFields() {
		return appendedJsonFields;
	}

	public void setAppendedJsonFields(JsonNode appendedJsonFields) {
		this.appendedJsonFields = appendedJsonFields;
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
