package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.Map;

public class CSourceRequest extends BaseRequest {
	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 3378608552494523957L;

	public CSourceRequest(String tenant, String id, Map<String, Object> requestPayload, int requestType) {
		super(tenant, id, requestPayload, requestType);
	}

	public CSourceRequest() {
	}
}
