package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.io.IOException;
import java.util.Map;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public class AppendHistoryEntityRequest extends HistoryEntityRequest {

	public AppendHistoryEntityRequest(String tenant, Map<String, Object> resolved, String entityId,
			BatchInfo batchInfo) {
		super(tenant, entityId, resolved, batchInfo, AppConstants.APPEND_TEMPORAL_REQUEST);

	}

	public AppendHistoryEntityRequest(BaseRequest entityRequest) throws ResponseException, IOException {
		this(entityRequest.getTenant(), entityRequest.getPayload(), entityRequest.getId(),
				entityRequest.getBatchInfo());

	}

}
