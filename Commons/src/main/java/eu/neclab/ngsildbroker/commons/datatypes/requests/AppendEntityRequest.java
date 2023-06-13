package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.Map;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;

public class AppendEntityRequest extends EntityRequest {

	public AppendEntityRequest(String tenant, String id, Map<String, Object> payload, BatchInfo batchInfo) {
		super(tenant, id, payload, batchInfo, AppConstants.APPEND_REQUEST);

	}
}
