package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;

import org.springframework.http.ResponseEntity;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.results.UpdateResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;

public class UpdateEntityRequest extends EntityRequest {

	private UpdateResult updateResult;

	public UpdateEntityRequest(ArrayListMultimap<String, String> headers, String id, Map<String, Object> entityBody,
			Map<String, Object> resolved, String attrName) throws ResponseException {
		super(headers, id, resolved, AppConstants.UPDATE_REQUEST);
		generateUpdate(resolved, entityBody, attrName);

	}

	private void generateUpdate(Map<String, Object> resolved, Map<String, Object> entityBody, String attrName)
			throws ResponseException {

		try {
			this.updateResult = updateFields(entityBody, resolved, attrName);

		} catch (ResponseException e) {
			throw e;
		} catch (Exception e) {
			throw new ResponseException(ErrorType.NotFound, e.getMessage());
		}

	}

	/**
	 * Method to merge/update fields in original Entitiy
	 * 
	 * @param entityBody
	 * @param resolved
	 * @param attrId
	 * @return
	 * @throws Exception
	 * @throws ResponseException
	 */
	@SuppressWarnings("unchecked")
	private UpdateResult updateFields(Map<String, Object> entityBody, Map<String, Object> resolved, String attrId)
			throws Exception, ResponseException {
		String now = SerializationTools.formatter.format(Instant.now());
		UpdateResult updateResult = new UpdateResult();

		if (attrId != null) {
			Object datasetId = resolved.get(NGSIConstants.NGSI_LD_DATA_SET_ID);
			if (!entityBody.containsKey(attrId)) {
				throw new ResponseException(ErrorType.NotFound, "Provided attribute is not present");
			}
			List<Map<String, Object>> list = ((List<Map<String, Object>>) entityBody.get(attrId));
			updateAttrib(list, resolved, datasetId, updateResult);
			Map<String, Object> tmp = new HashMap<String, Object>();
			tmp.put(attrId, getRequestPayload());
			setRequestPayload(tmp);
		} else {
			Object bodyId = resolved.get(NGSIConstants.JSON_LD_ID);
			if (bodyId != null && !getId().equals(bodyId)) {
				throw new ResponseException(ErrorType.BadRequestData,
						"The entity id in the url and in the payload must be the same.");
			}
			for (Entry<String, Object> entry : resolved.entrySet()) {
				String fieldName = entry.getKey();
				// TOP level updates of context id or type are ignored
				if (fieldName.equalsIgnoreCase(NGSIConstants.JSON_LD_CONTEXT)
						|| fieldName.equalsIgnoreCase(NGSIConstants.JSON_LD_ID)
						|| fieldName.equalsIgnoreCase(NGSIConstants.JSON_LD_TYPE)) {
					continue;
				}

				if (entityBody.containsKey(fieldName)) {
					List<Map<String, Object>> updateValueList = (List<Map<String, Object>>) entry.getValue();
					List<Map<String, Object>> originalValueList = (List<Map<String, Object>>) entityBody.get(fieldName);
					for (Map<String, Object> entry2 : updateValueList) {
						String updateDatasetId = null;
						if (entry2.containsKey(NGSIConstants.NGSI_LD_DATA_SET_ID)) {
							updateDatasetId = (String) (((List<Map<String, Object>>) entry2
									.get(NGSIConstants.NGSI_LD_DATA_SET_ID)).get(0)).get(NGSIConstants.JSON_LD_ID);
						}
						Map<String, Object> toRemove = null;
						for (Map<String, Object> entry3 : originalValueList) {
							String originalDatasetId = null;
							if (entry3.containsKey(NGSIConstants.NGSI_LD_DATA_SET_ID)) {
								originalDatasetId = (String) (((List<Map<String, Object>>) entry3
										.get(NGSIConstants.NGSI_LD_DATA_SET_ID)).get(0)).get(NGSIConstants.JSON_LD_ID);
							}
							if (updateDatasetId == null ^ originalDatasetId == null) {
								continue;
							}
							if ((updateDatasetId == null && originalDatasetId == null)
									|| updateDatasetId.equals(originalDatasetId)) {
								toRemove = entry3;
								break;
							}
						}
						if (toRemove == null) {
							String reason;
							if (updateDatasetId == null) {
								reason = "default entry not found";
							} else {
								reason = updateDatasetId + " datasetId not found";
							}
							updateResult.addToNotUpdated(fieldName, reason);
						} else {
							entry2.put(NGSIConstants.NGSI_LD_CREATED_AT,
									toRemove.get(NGSIConstants.NGSI_LD_CREATED_AT));
							setTemporalProperties(entry2, "", now, true);
							originalValueList.remove(toRemove);
							originalValueList.add(entry2);
							updateResult.addToUpdated(fieldName);
						}
					}
				} else {
					updateResult.addToNotUpdated(fieldName, "attribute not found in original entity");
				}
			}

		}
		setFinalPayload(entityBody);
		this.withSysAttrs = JsonUtils.toPrettyString(entityBody);
		removeTemporalProperties(entityBody);
		this.entityWithoutSysAttrs = JsonUtils.toPrettyString(entityBody);
		this.keyValue = JsonUtils.toPrettyString(getKeyValueEntity(entityBody));
		return updateResult;
	}

	private void updateAttrib(List<Map<String, Object>> list, Map<String, Object> update, Object datasetId,
			UpdateResult updateResult) throws ResponseException {
		boolean found = false;
		for (Map<String, Object> originalNode : list) {
			Object payloadDatasetId = null;
			if (originalNode.containsKey(NGSIConstants.NGSI_LD_DATA_SET_ID)) {
				payloadDatasetId = originalNode.get(NGSIConstants.NGSI_LD_DATA_SET_ID);
			}
			if (payloadDatasetId == null ^ datasetId == null) {
				continue;
			}
			if ((payloadDatasetId == null && datasetId == null) || payloadDatasetId.equals(datasetId)) {
				found = true;
				updateAttrib(originalNode, update, updateResult);
				break;
			}
		}

		if (!found) {
			throw new ResponseException(ErrorType.NotFound, "Provided datasetId is not present");
		}
	}

	private void updateAttrib(Map<String, Object> originalEntry, Map<String, Object> updateEntry,
			UpdateResult updateResult) throws ResponseException {
		for (Entry<String, Object> entry : updateEntry.entrySet()) {
			String key = entry.getKey();
			if (!originalEntry.containsKey(key)) {
				updateResult.addToNotUpdated(key, "entry not found in attribute");
			} else {
				validateValue(key, originalEntry.get(key), entry.getValue());
				originalEntry.put(key, entry.getValue());
				updateResult.addToUpdated(key);
			}
		}
	}

	private void validateValue(String key, Object originalObj, Object newObj) throws ResponseException {
		switch (key) {
			case NGSIConstants.JSON_LD_TYPE:
				if (!originalObj.equals(newObj)) {
					throw new ResponseException(ErrorType.BadRequestData, "The type of an attribute cannot be changed");
				}
				break;
			default:
				break;
		}
	}

	public UpdateResult getUpdateResult() {
		return updateResult;
	}

	public void setUpdateResult(UpdateResult updateResult) {
		this.updateResult = updateResult;
	}

}
