package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public class DeleteAttributeRequest extends EntityRequest {

	public DeleteAttributeRequest(ArrayListMultimap<String, String> headers, String entityId,
			Map<String, Object> entityBody, String attrId, String datasetId, String deleteAll)
			throws ResponseException {
		super(headers, entityId, entityBody, AppConstants.DELETE_ATTRIBUTE_REQUEST);
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

		if (entityBody.containsKey(attrId)) {
			if (deleteAll != null && !deleteAll.isEmpty() && deleteAll.equals("true")) {
				if (entityBody.remove(attrId) == null) {
					throw new ResponseException(ErrorType.NotFound, attrId + " not found");
				}
			} else {
				List<Map<String, Object>> myArray = (List<Map<String, Object>>) entityBody.get(attrId);
				Iterator<Map<String, Object>> it = myArray.iterator();
				boolean found = false;
				while (it.hasNext()) {
					Map<String, Object> entry = it.next();
					String payloadDatasetId = null;
					if (entry.containsKey(NGSIConstants.NGSI_LD_DATA_SET_ID)) {
						payloadDatasetId = (String) (((List<Map<String, Object>>) entry
								.get(NGSIConstants.NGSI_LD_DATA_SET_ID)).get(0)).get(NGSIConstants.JSON_LD_ID);
					}

					if (payloadDatasetId == null ^ datasetId == null) {
						continue;
					}
					if ((payloadDatasetId == null && datasetId == null) || payloadDatasetId.equals(datasetId)) {
						found = true;
						it.remove();
						break;
					}

				}
				if (!found) {
					throw new ResponseException(ErrorType.NotFound, "Provided datasetId is not present");
				}
				if (myArray.size() == 0) {
					entityBody.remove(attrId);
				}
			}
		} else {
			throw new ResponseException(ErrorType.NotFound, "Attribute is not present");
		}
		return entityBody;
	}

}
