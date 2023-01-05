package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.Map;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;

public class AppendCSourceRequest extends CSourceRequest {

	public AppendCSourceRequest(String tenant, String registrationId, Map<String, Object> update) {
		super(tenant, registrationId, update, null, AppConstants.APPEND_REQUEST);
	}
}
