package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.Map;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;

public class AppendHistoryEntityRequest extends BaseRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = -362534486980040536L;

	public AppendHistoryEntityRequest(String tenant, Map<String, Object> resolved, String entityId, boolean zipped) {
		super(tenant, entityId, resolved, AppConstants.APPEND_TEMPORAL_REQUEST, zipped);

	}

	public AppendHistoryEntityRequest(BaseRequest entityRequest) {
		super(entityRequest.getTenant(), entityRequest.getIds(), entityRequest.getPayload(),
				AppConstants.APPEND_TEMPORAL_REQUEST, entityRequest.isZipped());

	}

}
