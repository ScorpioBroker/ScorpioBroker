package eu.neclab.ngsildbroker.commons.datatypes.requests;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;

public class DeleteHistoryEntityRequest extends HistoryEntityRequest {

	public DeleteHistoryEntityRequest(String tenant, String entityId) {
		super(tenant, entityId, null, AppConstants.DELETE_TEMPORAL_REQUEST);
	}

	public DeleteHistoryEntityRequest(BaseRequest message) {
		this(message.getTenant(), message.getId());
	}

}
