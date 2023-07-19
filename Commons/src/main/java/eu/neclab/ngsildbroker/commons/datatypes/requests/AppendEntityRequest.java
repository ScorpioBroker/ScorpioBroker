package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.Map;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;

public class AppendEntityRequest extends EntityRequest {

	public AppendEntityRequest(String tenant, String id, Map<String, Object> payload) {
		super(tenant, id, payload, AppConstants.APPEND_REQUEST);

	}
}
