package eu.neclab.ngsildbroker.commons.datatypes.requests;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;

public class DeleteHistoryEntityRequest extends HistoryEntityRequest {

	public DeleteHistoryEntityRequest(String tenant, String entityId, BatchInfo batchInfo) {
		super(tenant, entityId, null, batchInfo, AppConstants.DELETE_TEMPORAL_REQUEST);
	}

	public DeleteHistoryEntityRequest(BaseRequest message) {
		this(message.getTenant(), message.getId(), message.getBatchInfo());
	}

}
