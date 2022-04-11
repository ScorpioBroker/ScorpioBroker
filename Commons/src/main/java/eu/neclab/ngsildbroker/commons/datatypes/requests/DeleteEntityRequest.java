package eu.neclab.ngsildbroker.commons.datatypes.requests;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public class DeleteEntityRequest extends EntityRequest {
	public DeleteEntityRequest() {
	}

	public DeleteEntityRequest(String entityId, ArrayListMultimap<String, String> headers) throws ResponseException {
		super(headers, entityId, null, AppConstants.DELETE_REQUEST);
		setFinalPayload(null);
		this.keyValue = "null";
		this.entityWithoutSysAttrs = "null";
		this.withSysAttrs = "null";

	}

}
