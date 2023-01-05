package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.Map;

import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;

public class CSourceRequest extends BaseRequest {
	
	
	public CSourceRequest(String tenant, String id, Map<String, Object> requestPayload, BatchInfo batchInfo, int requestType) {
		super(tenant, id, requestPayload, batchInfo, requestType);
	}

	public CSourceRequest() {
	}
}
