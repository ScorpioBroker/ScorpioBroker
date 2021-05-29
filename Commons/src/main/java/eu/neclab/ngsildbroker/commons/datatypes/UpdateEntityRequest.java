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

public class UpdateEntityRequest extends EntityRequest{

	private UpdateResult updateResult;

	public UpdateEntityRequest(ArrayListMultimap<String, String> headers, String id, String old, String update, String attrName) throws ResponseException {
		super(AppConstants.OPERATION_UPDATE_ENTITY, headers);
		this.id=id;
		generateUpdate(update, old, attrName);
	}

	private void generateUpdate(String update, String old, String attrName) throws ResponseException {
		JsonNode updateNode;
		try {
			updateNode = objectMapper.readTree(update);
			this.updateResult = updateFields(old, updateNode, attrName);
			this.entityWithoutSysAttrs = updateResult.getJsonWithoutSysAttrs();
			this.withSysAttrs = updateResult.getJson();
			this.keyValue = objectMapper.writeValueAsString(getKeyValueEntity(updateResult.getFinalNode()));
		} catch (Exception e) {
			throw new ResponseException(ErrorType.NotFound, e.getMessage());
		}
		
		
	}

	/**
	 * Method to merge/update fields in original Entitiy
	 * 
	 * @param originalJsonObject
	 * @param jsonToUpdate
	 * @param attrId
	 * @return
	 * @throws Exception
	 * @throws ResponseException
	 */
	private UpdateResult updateFields(String originalJsonObject, JsonNode jsonToUpdate, String attrId)
			throws Exception, ResponseException {
		logger.trace("updateFields() :: started");
		String now = SerializationTools.formatter.format(Instant.now());
		JsonNode resultJson = objectMapper.createObjectNode();
		UpdateResult updateResult = new UpdateResult(jsonToUpdate, resultJson);
		JsonNode node = objectMapper.readTree(originalJsonObject);
		ObjectNode objectNode = (ObjectNode) node;
		if (attrId != null) {
			if (objectNode.get(attrId) == null) {
				throw new ResponseException(ErrorType.NotFound, "Provided attribute is not present");
			}
			JsonNode originalNode = ((ArrayNode) objectNode.get(attrId)).get(0);
			if (((ObjectNode) originalNode).has(NGSIConstants.NGSI_LD_INSTANCE_ID)) {
				((ObjectNode) originalNode).remove(NGSIConstants.NGSI_LD_INSTANCE_ID);
			}
			JsonNode innerNode = ((ArrayNode) objectNode.get(attrId));
			ArrayNode myArray = (ArrayNode) innerNode;
			String availableDatasetId = null;

			for (int i = 0; i < myArray.size(); i++) {
				if (myArray.get(i).has(NGSIConstants.NGSI_LD_DATA_SET_ID)) {
					String payloadDatasetId = myArray.get(i).get(NGSIConstants.NGSI_LD_DATA_SET_ID).get(0)
							.get(NGSIConstants.JSON_LD_ID).asText();
					if (jsonToUpdate.has(NGSIConstants.NGSI_LD_DATA_SET_ID)) {
						String datasetId = jsonToUpdate.get(NGSIConstants.NGSI_LD_DATA_SET_ID).get(0)
								.get(NGSIConstants.JSON_LD_ID).asText();
						if (payloadDatasetId.equalsIgnoreCase(datasetId)) {
							availableDatasetId = "available";
							setFieldValue(jsonToUpdate.fieldNames(), ((ArrayNode) objectNode.get(attrId)), jsonToUpdate,
									updateResult, i);
						}
					} else {
						if (payloadDatasetId.equals(NGSIConstants.DEFAULT_DATA_SET_ID)) {
							setFieldValue(jsonToUpdate.fieldNames(), ((ArrayNode) objectNode.get(attrId)), jsonToUpdate,
									updateResult, i);
						}
					}
				} else {
					if (jsonToUpdate.has(NGSIConstants.NGSI_LD_DATA_SET_ID)) {
						((ObjectNode) innerNode.get(i)).putArray(NGSIConstants.NGSI_LD_DATA_SET_ID).addObject()
								.put(NGSIConstants.JSON_LD_ID, NGSIConstants.DEFAULT_DATA_SET_ID);
					} else {
						((ObjectNode) innerNode.get(i)).putArray(NGSIConstants.NGSI_LD_DATA_SET_ID).addObject()
								.put(NGSIConstants.JSON_LD_ID, NGSIConstants.DEFAULT_DATA_SET_ID);
						if (innerNode.get(i).has(NGSIConstants.NGSI_LD_MODIFIED_AT)) {
							((ObjectNode) innerNode.get(i)).remove(NGSIConstants.NGSI_LD_MODIFIED_AT);
							((ObjectNode) innerNode.get(i)).putArray(NGSIConstants.NGSI_LD_MODIFIED_AT).addObject()
									.put(NGSIConstants.JSON_LD_TYPE, NGSIConstants.NGSI_LD_DATE_TIME)
									.put(NGSIConstants.JSON_LD_VALUE, now);
						}
						setFieldValue(jsonToUpdate.fieldNames(), ((ArrayNode) objectNode.get(attrId)), jsonToUpdate,
								updateResult, i);
					}
				}
			}
			if (jsonToUpdate.has(NGSIConstants.NGSI_LD_DATA_SET_ID)) {
				if ((availableDatasetId == null) || (availableDatasetId.isEmpty())) {
					throw new ResponseException(ErrorType.NotFound, "Provided datasetId is not present");
				}
			}
		} else {
			Iterator<String> it = jsonToUpdate.fieldNames();
			while (it.hasNext()) {
				String field = it.next();
				// TOP level updates of context id or type are ignored
				if (field.equalsIgnoreCase(NGSIConstants.JSON_LD_CONTEXT)
						|| field.equalsIgnoreCase(NGSIConstants.JSON_LD_ID)
						|| field.equalsIgnoreCase(NGSIConstants.JSON_LD_TYPE)) {
					continue;
				}
				logger.trace("field: " + field);
				if (node.has(field)) {
					JsonNode originalNode = ((ArrayNode) objectNode.get(field)).get(0);
					JsonNode attrNode = jsonToUpdate.get(field).get(0);
					String createdAt = now;

					// keep original createdAt value if present in the original json
					if ((originalNode instanceof ObjectNode)
							&& ((ObjectNode) originalNode).has(NGSIConstants.NGSI_LD_CREATED_AT)
							&& ((ObjectNode) originalNode).get(NGSIConstants.NGSI_LD_CREATED_AT).isArray()) {
						createdAt = ((ObjectNode) ((ObjectNode) originalNode).get(NGSIConstants.NGSI_LD_CREATED_AT)
								.get(0)).get(NGSIConstants.JSON_LD_VALUE).asText();
					}
					setTemporalProperties(attrNode, createdAt, now, false);

					// TODO check if this should ever happen. 5.6.4.4 says BadRequest if AttrId is
					// present ...
					objectNode.replace(field, jsonToUpdate.get(field));
					((ObjectNode) updateResult.getAppendedJsonFields()).set(field, jsonToUpdate.get(field));
					logger.trace("appended json fields: " + updateResult.getAppendedJsonFields().toString());
					updateResult.setStatus(true);
				} else {
					// throw new ResponseException(ErrorType.NotFound);
				}
			}
		}
		setTemporalProperties(node, "", now, true); // root only, modifiedAt only
		updateResult.setJson(node.toString());
		updateResult.setFinalNode(node);
		removeTemporalProperties(node);
		updateResult.setJsonWithoutSysAttrs(node.toString());
		logger.trace("updateFields() :: completed");
		return updateResult;
	}

	public boolean getStatus() {
		return updateResult.getStatus();
	}

	public UpdateResult getUpdateResult() {
		return updateResult;
	}

	public void setUpdateResult(UpdateResult updateResult) {
		this.updateResult = updateResult;
	}
	
}
