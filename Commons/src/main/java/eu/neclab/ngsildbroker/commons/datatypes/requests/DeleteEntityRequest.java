package eu.neclab.ngsildbroker.commons.datatypes.requests;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;

public class DeleteEntityRequest extends EntityRequest {

	public DeleteEntityRequest() {
	}

	public DeleteEntityRequest(String tenant, String entityId) {
		super(tenant, entityId, null, AppConstants.DELETE_REQUEST);
	}

}
