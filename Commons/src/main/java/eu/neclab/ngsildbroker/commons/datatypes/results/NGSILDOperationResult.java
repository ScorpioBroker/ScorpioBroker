package eu.neclab.ngsildbroker.commons.datatypes.results;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import com.github.jsonldjava.core.Context;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public class NGSILDOperationResult {
	private int operationType;
	private String entityId;

	private boolean wasUpdated = false;

	private List<CRUDSuccess> successes = Lists.newArrayList();
	private List<ResponseException> failures = Lists.newArrayList();

	public NGSILDOperationResult(int operationType, String entityId) {
		super();
		this.operationType = operationType;
		this.entityId = entityId;
	}

	public List<CRUDSuccess> getSuccesses() {
		return successes;
	}

	public void setSuccesses(List<CRUDSuccess> successes) {
		this.successes = successes;
	}

	public void addSuccess(CRUDSuccess success) {
		this.successes.add(success);
	}

	public List<ResponseException> getFailures() {
		return failures;
	}

	public void setFailures(List<ResponseException> failures) {
		this.failures = failures;
	}

	public void addFailure(ResponseException failure) {
		this.failures.add(failure);
	}

	public String getEntityId() {
		return entityId;
	}

	public Map<String, Object> getJson() {
		Map<String, Object> result = Maps.newHashMap();
		result.put(NGSIConstants.ERROR_TYPE, getOperationType());
		result.put(NGSIConstants.QUERY_PARAMETER_ID, entityId);
		if (!successes.isEmpty()) {
			List<Object> temp = Lists.newArrayList();
			for (CRUDSuccess entry : successes) {
				temp.add(entry.getJson());
			}
			result.put("success", temp);
		}
		if (!failures.isEmpty()) {
			List<Object> temp = Lists.newArrayList();
			for (ResponseException entry : failures) {
				temp.add(entry.getJson());
			}
			result.put("failure", temp);
		}
		return result;
	}

	private String getOperationType() {
		switch (operationType) {
			case AppConstants.CREATE_REQUEST:
				return "Create";
			case AppConstants.UPDATE_REQUEST:
				return "Update";
			case AppConstants.APPEND_REQUEST:
				return "Append";
			case AppConstants.DELETE_REQUEST:
				return "Delete";
			case AppConstants.DELETE_ATTRIBUTE_REQUEST:
				return "Delete Attribute";
			case AppConstants.UPSERT_REQUEST:
				return "Upsert";
			case AppConstants.REPLACE_ENTITY_REQUEST:
				return "Replace Entity";
			case AppConstants.MERGE_PATCH_PAYLOAD:
				return "Merge";
			default:
				return "Unknown Operation";
		}
	}

	private static int getOperationCode(String operationType) {
		switch (operationType) {
			case "Create":
				return AppConstants.CREATE_REQUEST;
			case "Update":
				return AppConstants.UPDATE_REQUEST;
			case "Append":
				return AppConstants.APPEND_REQUEST;
			case "Delete":
				return AppConstants.DELETE_REQUEST;
			case "Delete Attribute":
				return AppConstants.DELETE_ATTRIBUTE_REQUEST;
			case "Upsert":
				return AppConstants.UPSERT_REQUEST;
			case "Replace Entity":
				return AppConstants.REPLACE_ENTITY_REQUEST;
		default:
			return -1;
		}
	}

	public static NGSILDOperationResult getFromUpdateResult() {
		return null;
	}

	public static NGSILDOperationResult getFromPayload(Map<String, Object> payload) throws ResponseException {
		// TODO some more content checks and error throwing if there is an unexpected
		// result
		String entityId = (String) payload.get(NGSIConstants.QUERY_PARAMETER_ID);
		int type = getOperationCode((String) payload.get(NGSIConstants.ERROR_TYPE));
		NGSILDOperationResult result = new NGSILDOperationResult(type, entityId);
		Object tmp = payload.get("success");
		if (tmp != null && tmp instanceof List) {
			List<Map<String, Object>> successList = (List<Map<String, Object>>) tmp;
			for (Map<String, Object> entry : successList) {
				result.addSuccess(CRUDSuccess.fromPayload(entry));
			}
		}
		tmp = payload.get("failure");
		if (tmp != null && tmp instanceof List) {
			List<Map<String, Object>> failList = (List<Map<String, Object>>) tmp;
			for (Map<String, Object> entry : failList) {
				result.addFailure(ResponseException.fromPayload(entry));
			}
		}
		return result;
	}

	public static Set<Attrib> getAttribs(Map<String, Object> entityAdded, Context context) {
		Set<Attrib> result = Sets.newHashSet();
		for (Entry<String, Object> entry : entityAdded.entrySet()) {
			String key = entry.getKey();
			if (key.equals(NGSIConstants.JSON_LD_ID) || key.equals(NGSIConstants.JSON_LD_TYPE)
					|| key.equals(NGSIConstants.NGSI_LD_CREATED_AT) || key.equals(NGSIConstants.NGSI_LD_OBSERVED_AT)
					|| key.equals(NGSIConstants.NGSI_LD_MODIFIED_AT)) {
				continue;
			}

			List<Map<String, Object>> tmp;
			if(entry.getValue() instanceof Map<?,?>){
				tmp = List.of((Map<String, Object>) entry.getValue());
			}else {
				tmp = ((List<Map<String, Object>>) entry.getValue());
			}
			Object datasetId;
			for (Map<String, Object> attribEntry : tmp) {
				datasetId = attribEntry.get(NGSIConstants.NGSI_LD_DATA_SET_ID);
				if (datasetId == null) {
					result.add(new Attrib(context.compactIri(key), null));
				} else {
					result.add(new Attrib(context.compactIri(key),
							((List<Map<String, String>>) datasetId).get(0).get(NGSIConstants.JSON_LD_ID)));
				}
			}

		}

		return result;
	}

	public boolean isWasUpdated() {
		return wasUpdated;
	}

	public void setWasUpdated(boolean wasUpdated) {
		this.wasUpdated = wasUpdated;
	}

}
