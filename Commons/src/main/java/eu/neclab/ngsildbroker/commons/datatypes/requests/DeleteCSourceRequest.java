package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.Map;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public class DeleteCSourceRequest extends CSourceRequest {

	/**
	 * constructor for serialization
	 */
	public DeleteCSourceRequest() {
	}

	public DeleteCSourceRequest(Map<String, Object> registration, String tenant, String registrationId)
			throws ResponseException {
		super(tenant, registrationId, registration, AppConstants.DELETE_REQUEST);
		setFinalPayload(registration);
	}

}
