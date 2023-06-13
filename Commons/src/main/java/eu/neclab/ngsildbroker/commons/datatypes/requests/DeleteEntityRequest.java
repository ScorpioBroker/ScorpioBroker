package eu.neclab.ngsildbroker.commons.datatypes.requests;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;

public class DeleteEntityRequest extends EntityRequest {

	public DeleteEntityRequest() {
	}

	public DeleteEntityRequest(String tenant, String entityId, BatchInfo batchInfo) {
		super(tenant, entityId, null, batchInfo, AppConstants.DELETE_REQUEST);
	}

}
