package eu.neclab.ngsildbroker.commons.datatypes.requests;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;

public class DeleteCSourceRequest extends CSourceRequest {

	/**
	 * constructor for serialization
	 */
	public DeleteCSourceRequest() {
	}

	public DeleteCSourceRequest(String tenant, String registrationId) {
		super(tenant, registrationId, null, null, AppConstants.DELETE_REQUEST);

	}

}
