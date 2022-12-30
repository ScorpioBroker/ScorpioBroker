package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.io.IOException;
import java.util.Map;

import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public class AppendHistoryEntityRequest extends AppendEntityRequest {

	public AppendHistoryEntityRequest(String tenant, Map<String, Object> resolved, String entityId, BatchInfo batchInfo)
			throws ResponseException {
		super(tenant, entityId, resolved, batchInfo);

	}

	public AppendHistoryEntityRequest(BaseRequest entityRequest) throws ResponseException, IOException {
		this(entityRequest.getTenant(), entityRequest.getPayload(), entityRequest.getId(),
				entityRequest.getBatchInfo());

	}

}
