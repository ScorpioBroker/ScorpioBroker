package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.Map;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public class DeleteEntityRequest extends EntityRequest {
	private Map<String, Object> oldEntity;

	public DeleteEntityRequest() {
	}

	public DeleteEntityRequest(String entityId, ArrayListMultimap<String, String> headers,
			Map<String, Object> oldEntity) throws ResponseException {
		super(headers, entityId, null, AppConstants.DELETE_REQUEST);
		setFinalPayload(null);
		this.keyValue = "null";
		this.entityWithoutSysAttrs = "null";
		this.withSysAttrs = "null";
		this.oldEntity = oldEntity;
	}

	public Map<String, Object> getOldEntity() {
		return oldEntity;
	}

	public void setOldEntity(Map<String, Object> oldEntity) {
		this.oldEntity = oldEntity;
	}

}
