package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.Map;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;

public class AppendEntityRequest extends BaseRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3507595034364109497L;

	public AppendEntityRequest(String tenant, String id, Map<String, Object> payload, boolean zipped) {
		super(tenant, id, payload, AppConstants.APPEND_REQUEST, zipped);

	}
}
