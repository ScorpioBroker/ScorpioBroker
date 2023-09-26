package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.Map;

public class EntityRequest extends BaseRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1963944233396659861L;

	public EntityRequest() {

	}

	EntityRequest(String tenant, String id, Map<String, Object> requestPayload, int requestType) {
		super(tenant, id, requestPayload, requestType);
	}
}
