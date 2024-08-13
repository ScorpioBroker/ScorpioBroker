package eu.neclab.ngsildbroker.commons.datatypes.requests;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;

public class DeleteCSourceRequest extends CSourceBaseRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = 68012134767909099L;

	/**
	 * constructor for serialization
	 */
	public DeleteCSourceRequest() {
	}

	public DeleteCSourceRequest(String tenant, String registrationId) {
		super(tenant, registrationId, null, AppConstants.DELETE_REQUEST);

	}

}
