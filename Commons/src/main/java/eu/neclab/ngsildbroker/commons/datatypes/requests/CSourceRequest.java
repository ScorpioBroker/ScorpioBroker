package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.Map;

public class CSourceRequest extends BaseRequest {
	
	
	public CSourceRequest(String tenant, String id, Map<String, Object> requestPayload, int requestType) {
		super(tenant, id, requestPayload, requestType);
	}

	public CSourceRequest() {
	}
}
