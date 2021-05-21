package eu.neclab.ngsildbroker.commons.datatypes;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.time.Instant;
import java.util.Iterator;

import org.springframework.messaging.MessageChannel;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;

public class AppendEntityRequest extends EntityRequest {

	private AppendResult appendResult;
	private String appendOverwriteFlag;

	public AppendEntityRequest(ArrayListMultimap<String, String> headers, String id, String old,
			String update, String overwriteOption, String appendOverwriteFlag) throws ResponseException {
		super(AppConstants.OPERATION_APPEND_ENTITY, headers);
		this.appendOverwriteFlag = appendOverwriteFlag;
		this.id=id;
		generateAppend(update, old, overwriteOption);
	}

	private void generateAppend(String update, String old, String overwriteOption) throws ResponseException {
		JsonNode updateNode;
		
		try {
			updateNode = objectMapper.readTree(update);
			this.appendResult = appendFields(old, updateNode, overwriteOption);
			this.entityWithoutSysAttrs = appendResult.getJsonWithoutSysAttrs();
			this.withSysAttrs = appendResult.getJson();
			this.keyValue = objectMapper.writeValueAsString(getKeyValueEntity(appendResult.getFinalNode()));
		} catch (Exception e) {
			throw new ResponseException(ErrorType.UnprocessableEntity, e.getMessage());
		}

	}

	
	/**
	 * Method to merge/append fileds in original Entity
	 * 
	 * @param originalJsonObject
	 * @param jsonToUpdate
	 * @return AppendResult
	 * @throws IOException
	 */
	private AppendResult appendFields(String originalJsonObject, JsonNode jsonToAppend, String overwriteOption)
			throws Exception {
		logger.trace("appendFields() :: started");
		String now = SerializationTools.formatter.format(Instant.now());
		JsonNode resultJson = objectMapper.createObjectNode();
		AppendResult appendResult = new AppendResult(jsonToAppend, resultJson);
		JsonNode node = objectMapper.readTree(originalJsonObject);
		ObjectNode objectNode = (ObjectNode) node;
		Iterator<String> it = jsonToAppend.fieldNames();
		while (it.hasNext()) {
			String key = it.next();
			if (key.equalsIgnoreCase(NGSIConstants.JSON_LD_CONTEXT) || key.equalsIgnoreCase(NGSIConstants.JSON_LD_ID)) {
				continue;
			}
			// remove if passed attribute have null value.
			if (jsonToAppend.get(key).isNull()) {
				objectNode.remove(key);
				((ObjectNode) appendResult.getAppendedJsonFields()).set(key, jsonToAppend.get(key));
				appendResult.setStatus(true);
				continue;
			}
			// validation append payload attribute
			/*
			 * if (!Validator.isValidAttribute(jsonToAppend.get(key))) { ((ObjectNode)
			 * appendResult.getAppendedJsonFields()).set(key, jsonToAppend.get(key));
			 * appendResult.setStatus(true); continue; }
			 */

			if ((objectNode.has(key) && !appendOverwriteFlag.equalsIgnoreCase(overwriteOption))
					|| !objectNode.has(key)) {
				if (jsonToAppend.get(key).isArray() && jsonToAppend.get(key).has(0)) {
					// TODO: should we keep the createdAt value if attribute already exists?
					// (overwrite operation) => if (objectNode.has(key)) ...
					JsonNode attrNode = jsonToAppend.get(key).get(0);
					setTemporalProperties(attrNode, now, now, false);
				}
				objectNode.replace(key, jsonToAppend.get(key));
				((ObjectNode) appendResult.getAppendedJsonFields()).set(key, jsonToAppend.get(key));
				appendResult.setStatus(true);
			}
		}
		setTemporalProperties(node, "", now, true); // root only, modifiedAt only
		appendResult.setJson(node.toString());

		removeTemporalProperties(node);
		appendResult.setJsonWithoutSysAttrs(node.toString());
		appendResult.setFinalNode(node);
		logger.trace("appendFields() :: completed");
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
