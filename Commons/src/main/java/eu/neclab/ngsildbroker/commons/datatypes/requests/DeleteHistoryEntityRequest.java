package eu.neclab.ngsildbroker.commons.datatypes.requests;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;

public class DeleteHistoryEntityRequest extends HistoryEntityRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6912588643063128002L;

	public DeleteHistoryEntityRequest(String tenant, String entityId) {
		super(tenant, entityId, null, AppConstants.DELETE_TEMPORAL_REQUEST);
	}

	public DeleteHistoryEntityRequest(BaseRequest message) {
		this(message.getTenant(), message.getId());
	}

}
