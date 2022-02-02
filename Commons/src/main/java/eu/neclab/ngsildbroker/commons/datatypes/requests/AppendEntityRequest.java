package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.results.UpdateResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;

public class AppendEntityRequest extends EntityRequest {

	private UpdateResult updateResult;

	public AppendEntityRequest(ArrayListMultimap<String, String> headers, String id, Map<String, Object> entityBody,
			Map<String, Object> resolved, String[] options) throws ResponseException {
		super(headers, id, resolved, AppConstants.APPEND_REQUEST);
		generateAppend(resolved, entityBody, options);
	}

	private void generateAppend(Map<String, Object> resolved, Map<String, Object> entityBody, String[] options)
			throws ResponseException {

		try {
			this.updateResult = appendFields(entityBody, resolved, options);
		} catch (IOException e) {
			throw new ResponseException(ErrorType.UnprocessableEntity, e.getMessage());
		}
	}

	/**
	 * Method to merge/append fileds in original Entity
	 * 
	 * @param entityBody
	 * @param jsonToUpdate
	 * @return AppendResult
	 * @throws JsonGenerationException
	 * @throws IOException
	 */

	@SuppressWarnings("unchecked")
	private UpdateResult appendFields(Map<String, Object> entityBody, Map<String, Object> resolved, String[] options)
			throws ResponseException, IOException {
		boolean overwrite = true;
		if (options != null && options.length > 0) {
			for (String option : options) {
				if (option.isBlank()) {
					continue;
				}
				if (option.equalsIgnoreCase(NGSIConstants.NO_OVERWRITE_OPTION)) {
					overwrite = false;
				} else {
					throw new ResponseException(ErrorType.BadRequestData, option + " is an invalid option");
				}
			}
		}
		String now = SerializationTools.formatter.format(Instant.now());
		Map<String, Object> resultJson = new HashMap<String, Object>();
		UpdateResult updateResult = new UpdateResult();
		for (Entry<String, Object> entry : resolved.entrySet()) {
			String key = entry.getKey();
			if (key.equalsIgnoreCase(NGSIConstants.JSON_LD_CONTEXT) || key.equalsIgnoreCase(NGSIConstants.JSON_LD_ID)
					|| key.equalsIgnoreCase(NGSIConstants.JSON_LD_TYPE)) {
				continue;
			}
			Object value = entry.getValue();
			if (value == null) {
				entityBody.remove(key);
				continue;
			}

			if (entityBody.containsKey(key)) {
				List<Map<String, Object>> updateValueList = (List<Map<String, Object>>) entry.getValue();
				List<Map<String, Object>> originalValueList = (List<Map<String, Object>>) entityBody.get(key);
				for (Map<String, Object> entry2 : updateValueList) {
					String updateDatasetId = null;
					if (entry2.containsKey(NGSIConstants.NGSI_LD_DATA_SET_ID)) {
						updateDatasetId = (String) (((List<Map<String, Object>>) entry2
								.get(NGSIConstants.NGSI_LD_DATA_SET_ID)).get(0)).get(NGSIConstants.JSON_LD_ID);
					}
					Map<String, Object> dateReferenceItem = entry2;
					boolean found = false;
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
							dateReferenceItem = entry3;
							found = true;
							break;
						}
					}
					if (!found || (found && overwrite)) {
						if (found) {
							originalValueList.remove(dateReferenceItem);
						}
						entry2.put(NGSIConstants.NGSI_LD_CREATED_AT,
								dateReferenceItem.get(NGSIConstants.NGSI_LD_CREATED_AT));
						setTemporalProperties(entry2, "", now, true);
						originalValueList.add(entry2);
						updateResult.addToUpdated(key);
						// entityBody.put(key, originalValueList);
					} else {
						String reason;
						if (updateDatasetId == null) {
							reason = "default entry is found";
						} else {
							reason = updateDatasetId + "  datasetId is found ";
						}
						updateResult.addToNotUpdated(key, reason);
					}

				}
			} else {
				setTemporalProperties(((List<Object>) value).get(0), now, now, false);
				entityBody.put(key, value);
				updateResult.addToUpdated(key);
				continue;

			}
		}
		setFinalPayload(entityBody);
		this.withSysAttrs = JsonUtils.toPrettyString(entityBody);
		removeTemporalProperties(entityBody);
		this.entityWithoutSysAttrs = JsonUtils.toPrettyString(entityBody);
		this.keyValue = JsonUtils.toPrettyString(getKeyValueEntity(entityBody));
		return updateResult;
	}

	public UpdateResult getUpdateResult() {
		return updateResult;
	}

	public void setUpdateResult(UpdateResult updateResult) {
		this.updateResult = updateResult;
	}

}
