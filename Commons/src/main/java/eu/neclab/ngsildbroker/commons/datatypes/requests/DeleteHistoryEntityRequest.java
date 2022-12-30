package eu.neclab.ngsildbroker.commons.datatypes.requests;

import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;

public class DeleteHistoryEntityRequest extends DeleteEntityRequest {

	public DeleteHistoryEntityRequest() {
	}

	public DeleteHistoryEntityRequest(String tenant, String entityId, BatchInfo batchInfo) {
		super(tenant, entityId, batchInfo);
	}

}
