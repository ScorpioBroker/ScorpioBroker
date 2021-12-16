package eu.neclab.ngsildbroker.commons.datatypes;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;

public class UpdateEntityRequest extends EntityRequest {

	private UpdateResult updateResult;

	public UpdateEntityRequest(ArrayListMultimap<String, String> headers, String id, Map<String, Object> entityBody,
			Map<String, Object> resolved, String attrName) throws ResponseException {
		super(AppConstants.OPERATION_UPDATE_ENTITY, headers);
		this.id = id;
		Object bodyId = resolved.get(NGSIConstants.JSON_LD_ID);
		if(bodyId != null) {
			throw new ResponseException(ErrorType.BadRequestData, "The entity id in the url and in the payload must be the same.");
		}
		generateUpdate(resolved, entityBody, attrName);
	}

	private void generateUpdate(Map<String, Object> resolved, Map<String, Object> entityBody, String attrName)
			throws ResponseException {

		try {

			this.updateResult = updateFields(entityBody, resolved, attrName);
			this.entityWithoutSysAttrs = updateResult.getJsonWithoutSysAttrs();

			if (attrName != null) {
				JsonElement jsonElement = new JsonParser().parse(entityWithoutSysAttrs);
				JsonObject top = jsonElement.getAsJsonObject();
				JsonElement jsonElement1 = top.get(attrName);
				JsonObject jsonObject = new JsonObject();
				jsonObject.add(attrName, jsonElement1);
				this.operationValue = jsonObject.toString();

			} else if (attrName == null) {
				this.operationValue = objectMapper.writeValueAsString(updateResult.getJsonToAppend());
			}

			this.withSysAttrs = updateResult.getJson();
			this.keyValue = objectMapper.writeValueAsString(getKeyValueEntity(updateResult.getFinalNode()));
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
	private UpdateResult updateFields(Map<String, Object> entityBody, Map<String, Object> resolved, String attrId)
			throws Exception, ResponseException {
		logger.trace("updateFields() :: started");
		String now = SerializationTools.formatter.format(Instant.now());
		Map<String, Object> resultJson = new HashMap<String, Object>();
		UpdateResult updateResult = new UpdateResult(resolved, resultJson);
//		JsonNode node = objectMapper.readTree(entityBody);
//		ObjectNode objectNode = (ObjectNode) node;
		if (attrId != null) {
			if (!entityBody.containsKey(attrId)) {
				throw new ResponseException(ErrorType.NotFound, "Provided attribute is not present");
			}
			List<Map<String, Object>> list = ((List<Map<String, Object>>) entityBody.get(attrId));
			String availableDatasetId = null;
			for (Map<String, Object> originalNode : list) {
				if (originalNode.containsKey(NGSIConstants.NGSI_LD_INSTANCE_ID)) {
					originalNode.remove(NGSIConstants.NGSI_LD_INSTANCE_ID);
				}
				if (originalNode.containsKey(NGSIConstants.NGSI_LD_DATA_SET_ID)) {
					String payloadDatasetId = (String) ((List<Map<String, Object>>) originalNode
							.get(NGSIConstants.NGSI_LD_DATA_SET_ID)).get(0).get(NGSIConstants.JSON_LD_ID);
					if (resolved.containsKey(NGSIConstants.NGSI_LD_DATA_SET_ID)) {
						String datasetId = (String) ((List<Map<String, Object>>) resolved
								.get(NGSIConstants.NGSI_LD_DATA_SET_ID)).get(0).get(NGSIConstants.JSON_LD_ID);
						if (payloadDatasetId.equalsIgnoreCase(datasetId)) {
							availableDatasetId = "available";
							originalNode.replace(attrId, resolved.get(attrId));
							// setFieldValue(resolved.fieldNames(), ((ArrayNode) objectNode.get(attrId)),
							// resolved,
							// updateResult, i);
						}
					} else {
						if (payloadDatasetId.equals(NGSIConstants.DEFAULT_DATA_SET_ID)) {
							Map<String, Object> replacement = (Map<String, Object>) resolved.get(attrId);
							if (replacement.containsKey(NGSIConstants.NGSI_LD_MODIFIED_AT)) {
								replacement.remove(NGSIConstants.NGSI_LD_MODIFIED_AT);
							}
							ArrayList<Object> tmp = new ArrayList<Object>();
							HashMap<String, Object> tmp2 = new HashMap<String, Object>();
							tmp2.put(NGSIConstants.JSON_LD_TYPE, NGSIConstants.NGSI_LD_DATE_TIME);
							tmp2.put(NGSIConstants.JSON_LD_VALUE, now);
							tmp.add(tmp2);
							replacement.put(NGSIConstants.NGSI_LD_MODIFIED_AT, tmp);
							originalNode.replace(attrId, replacement);
						}
					}
				} else {
					if (resolved.containsKey(NGSIConstants.NGSI_LD_DATA_SET_ID)) {
						ArrayList<Object> tmp = new ArrayList<Object>();
						HashMap<String, Object> tmp2 = new HashMap<String, Object>();
						tmp2.put(NGSIConstants.JSON_LD_ID, NGSIConstants.DEFAULT_DATA_SET_ID);
						tmp.add(tmp2);
						originalNode.put(NGSIConstants.NGSI_LD_DATA_SET_ID, tmp);
					} else {
						ArrayList<Object> tmp = new ArrayList<Object>();
						HashMap<String, Object> tmp2 = new HashMap<String, Object>();
						tmp2.put(NGSIConstants.JSON_LD_ID, NGSIConstants.DEFAULT_DATA_SET_ID);
						tmp.add(tmp2);
						originalNode.put(NGSIConstants.NGSI_LD_DATA_SET_ID, tmp);
						if (originalNode.containsKey(NGSIConstants.NGSI_LD_MODIFIED_AT)) {
							originalNode.remove(NGSIConstants.NGSI_LD_MODIFIED_AT);
							tmp = new ArrayList<Object>();
							tmp2 = new HashMap<String, Object>();
							tmp2.put(NGSIConstants.JSON_LD_TYPE, NGSIConstants.NGSI_LD_DATE_TIME);
							tmp2.put(NGSIConstants.JSON_LD_VALUE, now);
							tmp.add(tmp2);
							originalNode.put(NGSIConstants.NGSI_LD_MODIFIED_AT, tmp);
						}
						originalNode.replace(attrId, resolved.get(attrId));

						// setFieldValue(resolved.fieldNames(), ((ArrayNode) objectNode.get(attrId)),
						// resolved,
						// updateResult, i);
					}
				}
				updateResult.setStatus(true);
			}
			if (resolved.containsKey(NGSIConstants.NGSI_LD_DATA_SET_ID)) {
				if ((availableDatasetId == null) || (availableDatasetId.isEmpty())) {
					throw new ResponseException(ErrorType.NotFound, "Provided datasetId is not present");
				}
			}
		} else {
			for (Entry<String, Object> entry : resolved.entrySet()) {
				String field = entry.getKey();
				// TOP level updates of context id or type are ignored
				if (field.equalsIgnoreCase(NGSIConstants.JSON_LD_CONTEXT)
						|| field.equalsIgnoreCase(NGSIConstants.JSON_LD_ID)
						|| field.equalsIgnoreCase(NGSIConstants.JSON_LD_TYPE)) {
					continue;
				}
				logger.trace("field: " + field);
				if (entityBody.containsKey(field)) {
					Map<String, Object> originalNode = ((List<Map<String, Object>>) entityBody.get(field)).get(0);
					Map<String, Object> attrNode = ((List<Map<String, Object>>) resolved.get(field)).get(0);
					String createdAt = now;

					// keep original createdAt value if present in the original json
					if (originalNode.containsKey(NGSIConstants.NGSI_LD_CREATED_AT)) {
						createdAt = (String) ((List<Map<String, Object>>) originalNode
								.get(NGSIConstants.NGSI_LD_CREATED_AT)).get(0).get(NGSIConstants.JSON_LD_VALUE);
					}

					setTemporalProperties(attrNode, createdAt, now, false);

					// TODO check if this should ever happen. 5.6.4.4 says BadRequest if AttrId is
					// present ...
					entityBody.replace(field, resolved.get(field));
					updateResult.getAppendedJsonFields().put(field, resolved.get(field));
					logger.trace("appended json fields: " + updateResult.getAppendedJsonFields().toString());
					updateResult.setStatus(true);
				}
			}
		}
		setTemporalProperties(entityBody, "", now, true); // root only, modifiedAt only
		updateResult.setJson(JsonUtils.toString(entityBody));
		updateResult.setFinalNode(entityBody);
		removeTemporalProperties(entityBody);
		updateResult.setJsonWithoutSysAttrs(JsonUtils.toString(entityBody));
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
