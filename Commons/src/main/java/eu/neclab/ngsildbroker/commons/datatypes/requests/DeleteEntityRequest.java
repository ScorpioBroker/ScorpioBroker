package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.Map;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import io.vertx.core.json.JsonObject;

public class DeleteEntityRequest extends EntityRequest {
	private Map<String, Object> oldEntity;

	public DeleteEntityRequest() {
	}

	public DeleteEntityRequest(String entityId, String tenant, Map<String, Object> oldEntity) throws ResponseException {
		super(tenant, entityId, null, AppConstants.DELETE_REQUEST);
		setFinalPayload(null);
		this.keyValue = JsonObject.mapFrom(null);
		this.entityWithoutSysAttrs = JsonObject.mapFrom(null);
		this.withSysAttrs = JsonObject.mapFrom(null);
		this.oldEntity = oldEntity;
	}

	public Map<String, Object> getOldEntity() {
		return oldEntity;
	}

	public void setOldEntity(Map<String, Object> oldEntity) {
		this.oldEntity = oldEntity;
	}

}
