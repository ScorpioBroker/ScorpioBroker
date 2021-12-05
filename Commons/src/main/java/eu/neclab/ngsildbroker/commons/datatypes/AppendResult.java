package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

public class AppendResult {
	
	private Map<String, Object> jsonToAppend;
	private Map<String, Object> appendedJsonFields;// = new ArrayList<JsonNode>();
	private Map<String, Object> finalNode;
	private boolean status=false;
	private String json;
	private String jsonWithoutSysAttrs;

	public boolean getAppendResult() {
		return jsonToAppend.size()==appendedJsonFields.size();
	}
	
	public AppendResult(Map<String, Object> resolved,Map<String, Object> resultJson) {
		super();
		this.jsonToAppend = resolved;
		this.appendedJsonFields=resultJson;
	}

	public Map<String, Object> getFinalNode() {
		return finalNode;
	}

	public void setFinalNode(Map<String, Object> finalNode) {
		this.finalNode = finalNode;
	}

	public Map<String, Object> getJsonToAppend() {
		return jsonToAppend;
	}

	public void setJsonToAppend(Map<String, Object> jsonToAppend) {
		this.jsonToAppend = jsonToAppend;
	}

	public Map<String, Object> getAppendedJsonFields() {
		return appendedJsonFields;
	}

	public void setAppendedJsonFields(Map<String, Object> appendedJsonFields) {
		this.appendedJsonFields = appendedJsonFields;
	}

	public boolean getStatus() {
		return status;
	}

	public void setStatus(boolean status) {
		this.status = status;
	}

	public String getJson() {
		return json;
	}

	public void setJson(String json) {
		this.json = json;
	}

	public String getJsonWithoutSysAttrs() {
		return jsonWithoutSysAttrs;
	}

	public void setJsonWithoutSysAttrs(String jsonWithoutSysAttrs) {
		this.jsonWithoutSysAttrs = jsonWithoutSysAttrs;
	}
	
}
