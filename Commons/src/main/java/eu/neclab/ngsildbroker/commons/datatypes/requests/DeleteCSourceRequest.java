package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.Map;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public class DeleteCSourceRequest extends CSourceRequest {

	/**
	 * constructor for serialization
	 */
	public DeleteCSourceRequest() {
	}

	public DeleteCSourceRequest(String tenant, Map<String, Object> registration, String registrationId)
			throws ResponseException {
		super(tenant, registrationId, registration, null, AppConstants.DELETE_REQUEST);

	}

}
