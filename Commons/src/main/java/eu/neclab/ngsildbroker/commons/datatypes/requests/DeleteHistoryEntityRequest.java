package eu.neclab.ngsildbroker.commons.datatypes.requests;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;

public class DeleteHistoryEntityRequest extends BaseRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6912588643063128002L;

	public DeleteHistoryEntityRequest(String tenant, String entityId, boolean zipped) {
		super(tenant, entityId, null, AppConstants.DELETE_TEMPORAL_REQUEST, zipped);
	}

	public DeleteHistoryEntityRequest(BaseRequest message) {
		super(message.getTenant(), message.getIds(), message.getPayload(), AppConstants.DELETE_TEMPORAL_REQUEST, message.isZipped());
	}

}
