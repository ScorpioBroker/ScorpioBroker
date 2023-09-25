package eu.neclab.ngsildbroker.commons.datatypes.requests;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;

public class DeleteEntityRequest extends EntityRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6604422774913112354L;

	public DeleteEntityRequest() {
	}

	public DeleteEntityRequest(String tenant, String entityId) {
		super(tenant, entityId, null, AppConstants.DELETE_REQUEST);
	}

}
