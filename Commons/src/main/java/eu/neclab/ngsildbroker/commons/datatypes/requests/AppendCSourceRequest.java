package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.Map;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;

public class AppendCSourceRequest extends CSourceBaseRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1055145743049669940L;

	public AppendCSourceRequest(String tenant, String registrationId, Map<String, Object> update) {
		super(tenant, registrationId, update, AppConstants.APPEND_REQUEST);
	}
}
