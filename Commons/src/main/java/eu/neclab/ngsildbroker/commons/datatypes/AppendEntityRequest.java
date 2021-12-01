package eu.neclab.ngsildbroker.commons.datatypes;

import java.io.IOException;
import java.time.Instant;
import java.util.Iterator;

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

	public AppendEntityRequest(ArrayListMultimap<String, String> headers, String id, String old, String update,
			String overwriteOption, String appendOverwriteFlag) throws ResponseException {
		super(AppConstants.OPERATION_APPEND_ENTITY, headers);
		this.appendOverwriteFlag = appendOverwriteFlag;
		this.id = id;
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
			this.operationValue = objectMapper.writeValueAsString(appendResult.getJsonToAppend());
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
					
			if (objectNode.has(key) && ((ArrayNode) objectNode.get(key)).size() > 1) {
				JsonNode innerNode = ((ArrayNode) objectNode.get(key));
				ArrayNode myArray = (ArrayNode) innerNode;
				boolean appendpayload = true;
				for (int i = 0; i < myArray.size(); i++) {
					if (myArray.get(i).has(NGSIConstants.NGSI_LD_DATA_SET_ID)) {

						String payloadDatasetId = myArray.get(i).get(NGSIConstants.NGSI_LD_DATA_SET_ID).get(0)
								.get(NGSIConstants.JSON_LD_ID).asText();
						if (jsonToAppend.get(key).get(0).has(NGSIConstants.NGSI_LD_DATA_SET_ID)) {
							String datasetId = jsonToAppend.get(key).get(0).get(NGSIConstants.NGSI_LD_DATA_SET_ID)
									.get(0).get(NGSIConstants.JSON_LD_ID).asText();
							if (payloadDatasetId.equalsIgnoreCase(datasetId)) {
								appendpayload = false;
								throw new ResponseException(ErrorType.AlreadyExists);

							}
						} else {
							appendpayload = true;
						}

					} else {
						if (jsonToAppend.get(key).get(0).has(NGSIConstants.NGSI_LD_DATA_SET_ID)) {
							appendpayload = true;
						} else {
							appendpayload = false;
							throw new ResponseException(ErrorType.AlreadyExists);
						}

					}
				}
				if (appendpayload == true) {
					myArray.insert(myArray.size(), jsonToAppend.get(key).get(0));
					appendResult.setStatus(true);
				}
			}

			else {

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
