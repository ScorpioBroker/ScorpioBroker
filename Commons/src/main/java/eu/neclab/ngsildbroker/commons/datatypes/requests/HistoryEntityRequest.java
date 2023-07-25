package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.Map;

public class HistoryEntityRequest extends EntityRequest {
	HistoryEntityRequest(String tenant, String id, Map<String, Object> requestPayload, int requestType) {
		super(tenant, id, requestPayload, requestType);
	}
}
