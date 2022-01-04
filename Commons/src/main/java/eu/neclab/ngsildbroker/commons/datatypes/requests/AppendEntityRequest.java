package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.results.AppendResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;

public class AppendEntityRequest extends EntityRequest {

	private AppendResult appendResult;

	public AppendEntityRequest(ArrayListMultimap<String, String> headers, String id, Map<String, Object> entityBody,
			Map<String, Object> resolved, String[] options) throws ResponseException {
		super(headers, id, resolved, AppConstants.APPEND_REQUEST);
		generateAppend(resolved, entityBody, options);
	}

	private void generateAppend(Map<String, Object> resolved, Map<String, Object> entityBody, String[] options)
			throws ResponseException {

		try {
			this.appendResult = appendFields(entityBody, resolved, options);
			this.entityWithoutSysAttrs = appendResult.getJsonWithoutSysAttrs();
			this.withSysAttrs = appendResult.getJson();
			this.keyValue = JsonUtils.toPrettyString(getKeyValueEntity(appendResult.getFinalNode()));
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

	@SuppressWarnings("unchecked")
	private AppendResult appendFields(Map<String, Object> entityBody, Map<String, Object> resolved, String[] options)
			throws Exception {
		boolean overwrite = true;
		if (options != null && options.length > 0) {
			for (String option : options) {
				if (option.isBlank()) {
					continue;
				}
				if (option.equalsIgnoreCase(NGSIConstants.NO_OVERWRITE_OPTION)) {
					overwrite = false;
				} else if (option.equalsIgnoreCase(NGSIConstants.OVERWRITE_OPTION) || option.equalsIgnoreCase(NGSIConstants.REPLACE_OPTION )) {
					overwrite = true;
				} else {
					throw new ResponseException(ErrorType.BadRequestData, option + " is an invalid option");
				}
			}
		}
		String now = SerializationTools.formatter.format(Instant.now());
		Map<String, Object> resultJson = new HashMap<String, Object>();
		AppendResult appendResult = new AppendResult(resolved, resultJson);
		appendResult.setStatus(true);
		for (Entry<String, Object> entry : resolved.entrySet()) {
			String key = entry.getKey();
			if (key.equalsIgnoreCase(NGSIConstants.JSON_LD_CONTEXT) || key.equalsIgnoreCase(NGSIConstants.JSON_LD_ID)
					|| key.equalsIgnoreCase(NGSIConstants.JSON_LD_TYPE)) {
				continue;
			}
			Object value = entry.getValue();
			if (value == null) {
				entityBody.remove(key);
				appendResult.getAppendedJsonFields().put(key, value);
				continue;
			}

			List<Map<String, Object>> list = ((List<Map<String, Object>>) entityBody.get(key));
			Map<String, Object> attrNode = ((List<Map<String, Object>>) resolved.get(key)).get(0);
			boolean appendpayload = true;
			if (entityBody.containsKey(key) && list.size() > 1) {
				for (Map<String, Object> originalNode : list) {
					if (originalNode.containsKey(NGSIConstants.NGSI_LD_DATA_SET_ID)) {
						String payloadDatasetId = (String) ((List<Map<String, Object>>) originalNode
								.get(NGSIConstants.NGSI_LD_DATA_SET_ID)).get(0).get(NGSIConstants.JSON_LD_ID);
						if (entry.getValue().toString().contains(NGSIConstants.NGSI_LD_DATA_SET_ID)) {
							String datasetId = (String) ((List<Map<String, Object>>) attrNode
									.get(NGSIConstants.NGSI_LD_DATA_SET_ID)).get(0).get(NGSIConstants.JSON_LD_ID);
							if (payloadDatasetId.equalsIgnoreCase(datasetId)) {
								appendpayload = false;
								throw new ResponseException(ErrorType.AlreadyExists, datasetId);
							}

						} else {
							appendpayload = true;
						}
					} else {
						if (attrNode.containsKey(NGSIConstants.NGSI_LD_DATA_SET_ID)) {
							appendpayload = true;
						} else {
							appendpayload = false;
							throw new ResponseException(ErrorType.AlreadyExists, key);
						}

					}

				}
				if (appendpayload == true) {
					list.add(attrNode);

					appendResult.setStatus(true);
				}
			}

			else {

				if ((entityBody.containsKey(key) && overwrite) || !entityBody.containsKey(key)) {
					if (value instanceof List && !((List<Object>) value).isEmpty()) {
						// TODO: should we keep the createdAt value if attribute already exists?
						// (overwrite operation) => if (objectNode.has(key)) ...
						setTemporalProperties(((List<Object>) value).get(0), now, now, false);
					}
					entityBody.put(key, value);
					appendResult.getAppendedJsonFields().put(key, value);
					continue;

				}
			}
			appendResult.setStatus(false);
		}
		setTemporalProperties(entityBody, "", now, true); // root only, modifiedAt only
		setFinalPayload(entityBody);
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
