package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.Map;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;

public class AppendHistoryEntityRequest extends HistoryEntityRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = -362534486980040536L;

	public AppendHistoryEntityRequest(String tenant, Map<String, Object> resolved, String entityId) {
		super(tenant, entityId, resolved, AppConstants.APPEND_TEMPORAL_REQUEST);

	}

	public AppendHistoryEntityRequest(BaseRequest entityRequest) {
		this(entityRequest.getTenant(), entityRequest.getPayload(), entityRequest.getId());

	}

}
