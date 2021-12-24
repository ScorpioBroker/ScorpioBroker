package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public class DeleteAttributeRequest extends EntityRequest {



	public DeleteAttributeRequest(ArrayListMultimap<String, String> headers, String entityId,
			Map<String, Object> entityBody, String attrId, String datasetId, String deleteAll)
			throws ResponseException {
		super(headers, entityId, entityBody);
		generateDeleteAttrib(entityId, entityBody, attrId, datasetId, deleteAll);
	}

	private void generateDeleteAttrib(String entityId, Map<String, Object> entityBody, String attrId, String datasetId,
			String deleteAll) throws ResponseException {
		try {
			Map<String, Object> finalJson = deleteFields(entityBody, attrId, datasetId, deleteAll);
			setFinalPayload(finalJson);
			this.withSysAttrs = JsonUtils.toPrettyString(finalJson);
			removeTemporalProperties(finalJson);
			this.entityWithoutSysAttrs = JsonUtils.toPrettyString(finalJson);
			this.keyValue = JsonUtils.toPrettyString(getKeyValueEntity(finalJson));
		} catch (Exception e) {
			throw new ResponseException(ErrorType.NotFound, e.getMessage());
		}
	}

	/**
	 * Method to delete attributes from original Entity
	 * 
	 * @param entityBody
	 * @param attrId
	 * @return
	 * @throws IOException
	 * @throws ResponseException
	 */
	@SuppressWarnings("unchecked")
	private Map<String, Object> deleteFields(Map<String, Object> entityBody, String attrId, String datasetId,
			String deleteAll) throws Exception, ResponseException {
		// ArrayNode myArray = (ArrayNode) innerNode;

		String availableDatasetId = null;
		if (entityBody.containsKey(attrId)) {
			// below condition remove the existing datasetId
			List<Map<String, Object>> myArray = (List<Map<String, Object>>) entityBody.get(attrId);
			if (datasetId != null && !datasetId.isEmpty()) {
				Iterator<Map<String, Object>> it = myArray.iterator();
				while (it.hasNext()) {
					Map<String, Object> entry = it.next();
					if (entry.containsKey(NGSIConstants.NGSI_LD_DATA_SET_ID)) {
						String payloadDatasetId = (String) (((List<Map<String, Object>>) entry
								.get(NGSIConstants.NGSI_LD_DATA_SET_ID)).get(0)).get(NGSIConstants.JSON_LD_ID);
						if (payloadDatasetId.equals(datasetId)) {
							availableDatasetId = "available";
							it.remove();
						}
					}
				}
				if ((availableDatasetId == null) || (availableDatasetId.isEmpty())) {
					throw new ResponseException(ErrorType.NotFound, "Provided datasetId is not present");
				}
				// below condition remove all the datasetId
			} else if (deleteAll != null && !deleteAll.isEmpty() && deleteAll.equals("true")) {
				if (entityBody.remove(attrId) == null) {
					throw new ResponseException(ErrorType.NotFound, attrId + " not found");
				}
			} else {
				// below condition remove the default datasetId
				Iterator<Map<String, Object>> it = myArray.iterator();
				while (it.hasNext()) {
					Map<String, Object> entry = it.next();
					if (entry.containsKey(NGSIConstants.NGSI_LD_DATA_SET_ID)) {
						String payloadDatasetId = ((List<Map<String, Object>>) entry
								.get(NGSIConstants.NGSI_LD_DATA_SET_ID)).get(0).get(NGSIConstants.JSON_LD_ID)
										.toString();
						if (payloadDatasetId.equals(NGSIConstants.DEFAULT_DATA_SET_ID)) {
							availableDatasetId = "available";
							it.remove();
						}
					} else {
						availableDatasetId = "NotAvailable";
						it.remove();
					}
				}
				if ((availableDatasetId == null) || (availableDatasetId.isEmpty())) {
					throw new ResponseException(ErrorType.NotFound, "Default attribute instance is not present");
				}
			}
			if (myArray.size() == 0) {
				entityBody.remove(attrId);
			}
		} else {
			throw new ResponseException(ErrorType.NotFound, "Attribute is not present");
		}
		return entityBody;
	}

}
