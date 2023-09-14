package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.Map;

public class HistoryEntityRequest extends EntityRequest {
	/**
	 * 
	 */
	private static final long serialVersionUID = -934387471264920898L;

	HistoryEntityRequest(String tenant, String id, Map<String, Object> requestPayload, int requestType) {
		super(tenant, id, requestPayload, requestType);
	}
}
