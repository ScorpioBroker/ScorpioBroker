package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.Map;

import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;

public class CreateHistoryEntityRequest extends CreateEntityRequest {

	public CreateHistoryEntityRequest(String tenant, Map<String, Object> resolved, BatchInfo batchInfo) {
		super(tenant, resolved, batchInfo);
	}

	public CreateHistoryEntityRequest(BaseRequest message) {
		super(message.getTenant(), message.getPayload(), message.getBatchInfo());
	}

}
