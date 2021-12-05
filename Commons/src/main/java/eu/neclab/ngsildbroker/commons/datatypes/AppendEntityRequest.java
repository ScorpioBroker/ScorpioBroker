package eu.neclab.ngsildbroker.commons.datatypes;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;

public class AppendEntityRequest extends EntityRequest {

	private AppendResult appendResult;
	private String appendOverwriteFlag;

	public AppendEntityRequest(ArrayListMultimap<String, String> headers, String id, Map<String, Object> entityBody,
			Map<String, Object> resolved, String overwriteOption, String appendOverwriteFlag) throws ResponseException {
		super(AppConstants.OPERATION_APPEND_ENTITY, headers);
		this.appendOverwriteFlag = appendOverwriteFlag;
		this.id = id;
		generateAppend(resolved, entityBody, overwriteOption);
	}

	private void generateAppend(Map<String, Object> resolved, Map<String, Object> entityBody, String overwriteOption)
			throws ResponseException {
		JsonNode updateNode;

		try {

			this.appendResult = appendFields(entityBody, resolved, overwriteOption);
			this.entityWithoutSysAttrs = appendResult.getJsonWithoutSysAttrs();
			this.withSysAttrs = appendResult.getJson();
			this.keyValue = JsonUtils.toPrettyString(getKeyValueEntity(appendResult.getFinalNode()));
			this.operationValue = JsonUtils.toPrettyString(appendResult.getJsonToAppend());
		} catch (Exception e) {
			throw new ResponseException(ErrorType.UnprocessableEntity, e.getMessage());
		}

	}

	/**
	 * Method to merge/append fileds in original Entity
	 * 
	 * @param entityBody
	 * @param jsonToUpdate
	 * @return AppendResult
	 * @throws IOException
	 */
	private AppendResult appendFields(Map<String, Object> entityBody, Map<String, Object> resolved,
			String overwriteOption) throws Exception {
		logger.trace("appendFields() :: started");
		String now = SerializationTools.formatter.format(Instant.now());
		Map<String, Object> resultJson = new HashMap<String, Object>();
		AppendResult appendResult = new AppendResult(resolved, resultJson);
		for (Entry<String, Object> entry : resolved.entrySet()) {
			String key = entry.getKey();
			if (key.equalsIgnoreCase(NGSIConstants.JSON_LD_CONTEXT) || key.equalsIgnoreCase(NGSIConstants.JSON_LD_ID)) {
				continue;
			}
			Object value = entry.getValue();
			if (value == null) {
				entityBody.remove(key);
				appendResult.getAppendedJsonFields().put(key, value);
				appendResult.setStatus(true);
				continue;
			}
			if ((entityBody.containsKey(key) && !appendOverwriteFlag.equalsIgnoreCase(overwriteOption))
					|| !entityBody.containsKey(key)) {
				if (value instanceof List && !((List) value).isEmpty()) {
					// TODO: should we keep the createdAt value if attribute already exists?
					// (overwrite operation) => if (objectNode.has(key)) ...
					setTemporalProperties(((List) value).get(0), now, now, false);
				}
				entityBody.put(key, value);
				appendResult.getAppendedJsonFields().put(key, value);
				appendResult.setStatus(true);
			}
		}
		setTemporalProperties(entityBody, "", now, true); // root only, modifiedAt only
		appendResult.setJson(JsonUtils.toPrettyString(entityBody));
		removeTemporalProperties(entityBody);
		appendResult.setJsonWithoutSysAttrs(JsonUtils.toPrettyString(entityBody));
		appendResult.setFinalNode(entityBody);
		return appendResult;
	}

	public boolean getStatus() {
		return appendResult.getStatus();
	}

	public AppendResult getAppendResult() {
		return appendResult;
	}

	public void setAppendResult(AppendResult appendResult) {
		this.appendResult = appendResult;
	}

}
