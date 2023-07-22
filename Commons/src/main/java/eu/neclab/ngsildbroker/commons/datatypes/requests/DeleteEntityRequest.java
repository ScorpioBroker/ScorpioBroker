package eu.neclab.ngsildbroker.commons.datatypes.requests;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;

public class DeleteEntityRequest extends EntityRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7407400931440687569L;

	public DeleteEntityRequest() {
	}

	public DeleteEntityRequest(String tenant, String entityId, BatchInfo batchInfo) {
		super(tenant, entityId, null, batchInfo, AppConstants.DELETE_REQUEST);
	}

}
