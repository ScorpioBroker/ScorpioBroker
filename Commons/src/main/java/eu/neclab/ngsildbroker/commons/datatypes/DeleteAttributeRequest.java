package eu.neclab.ngsildbroker.commons.datatypes;

import java.io.IOException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public class DeleteAttributeRequest extends EntityRequest {

	private AppendResult appendResult;
	private String appendOverwriteFlag;

	public DeleteAttributeRequest(ArrayListMultimap<String, String> headers, String entityId, String entityBody,
			String attrId, String datasetId, String deleteAll) throws ResponseException {
		super(AppConstants.OPERATION_DELETE_ATTRIBUTE_ENTITY, headers);
		this.id = entityId;
		generateDeleteAttrib(entityId, entityBody, attrId, datasetId, deleteAll);
	}

	private void generateDeleteAttrib(String entityId, String entityBody, String attrId, String datasetId,
			String deleteAll) throws ResponseException {
		try {
			JsonNode finalJson = deleteFields(entityBody, attrId, datasetId, deleteAll);
			this.withSysAttrs = objectMapper.writeValueAsString(finalJson);
			removeTemporalProperties(finalJson);
			this.entityWithoutSysAttrs = objectMapper.writeValueAsString(finalJson);
			this.keyValue = objectMapper.writeValueAsString(getKeyValueEntity(finalJson));
		} catch (Exception e) {
			throw new ResponseException(ErrorType.NotFound, e.getMessage());
		}
	}

	/**
	 * Method to delete attributes from original Entity
	 * 
	 * @param originalJsonObject
	 * @param attrId
	 * @return
	 * @throws IOException
	 * @throws ResponseException
	 */
	private JsonNode deleteFields(String originalJsonObject, String attrId, String datasetId, String deleteAll)
			throws Exception, ResponseException {
		logger.trace("deleteFields() :: started");
		JsonNode node = objectMapper.readTree(originalJsonObject);
		ObjectNode objectNode = (ObjectNode) node;
		JsonNode innerNode = ((ArrayNode) objectNode.get(attrId));
		ArrayNode myArray = (ArrayNode) innerNode;
		String availableDatasetId = null;
		if (objectNode.has(attrId)) {
			// below condition remove the existing datasetId
			if (datasetId != null && !datasetId.isEmpty()) {
				for (int i = 0; i < myArray.size(); i++) {
					if (myArray.get(i).has(NGSIConstants.NGSI_LD_DATA_SET_ID)) {
						String payloadDatasetId = myArray.get(i).get(NGSIConstants.NGSI_LD_DATA_SET_ID).get(0)
								.get(NGSIConstants.JSON_LD_ID).asText();
						if (payloadDatasetId.equals(datasetId)) {
							availableDatasetId = "available";
							myArray.remove(i);
						}
					}
				}
				if ((availableDatasetId == null) || (availableDatasetId.isEmpty())) {
					throw new ResponseException(ErrorType.NotFound, "Provided datasetId is not present");
				}
				// below condition remove all the datasetId
			} else if (deleteAll != null && !deleteAll.isEmpty()) {
				if (deleteAll.equals("true")) {
					if (objectNode.has(attrId)) {
						objectNode.remove(attrId);
					} else {
						throw new ResponseException(ErrorType.NotFound);
					}
				} else {
					throw new ResponseException(ErrorType.InvalidRequest, "request is not valid");
				}
			} else {
				// below condition remove the default datasetId
				for (int i = 0; i < myArray.size(); i++) {
					if (myArray.get(i).has(NGSIConstants.NGSI_LD_DATA_SET_ID)) {
						String payloadDatasetId = myArray.get(i).get(NGSIConstants.NGSI_LD_DATA_SET_ID).get(0)
								.get(NGSIConstants.JSON_LD_ID).asText();
						if (payloadDatasetId.equals(NGSIConstants.DEFAULT_DATA_SET_ID)) {
							availableDatasetId = "available";
							myArray.remove(i);
						}
					} else {
						availableDatasetId = "NotAvailable";
						myArray.remove(i);
					}
				}
				if ((availableDatasetId == null) || (availableDatasetId.isEmpty())) {
					throw new ResponseException(ErrorType.NotFound, "Default attribute instance is not present");
				}
			}
			if (myArray.size() == 0) {
				if (objectNode.has(attrId)) {
					objectNode.remove(attrId);
				}
			}
		} else {
			throw new ResponseException(ErrorType.NotFound, "Attribute is not present");
		}
		logger.trace("deleteFields() :: completed");
		return objectNode;
	}

}
