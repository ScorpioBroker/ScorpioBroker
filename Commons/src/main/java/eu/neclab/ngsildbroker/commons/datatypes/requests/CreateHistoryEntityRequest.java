package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.Map;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;

public class CreateHistoryEntityRequest extends HistoryEntityRequest {

	public CreateHistoryEntityRequest(String tenant, Map<String, Object> resolved, BatchInfo batchInfo) {
		super(tenant, (String) resolved.get(NGSIConstants.JSON_LD_ID), resolved, batchInfo,
				AppConstants.CREATE_TEMPORAL_REQUEST);
	}

	public CreateHistoryEntityRequest(BaseRequest message) {
		this(message.getTenant(), message.getPayload(), message.getBatchInfo());
	}

}
