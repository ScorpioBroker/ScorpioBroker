package eu.neclab.ngsildbroker.commons.datatypes.requests;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public class DeleteHistoryEntityRequest extends HistoryEntityRequest {

	private String resolvedAttrId;
	private String instanceId;

	public DeleteHistoryEntityRequest() {
	}

	public DeleteHistoryEntityRequest(String tenant, String resolvedAttrId, String instanceId, String entityId,
			BatchInfo batchInfo) throws ResponseException {
		super(tenant, null, entityId, batchInfo, AppConstants.DELETE_REQUEST);
		this.resolvedAttrId = resolvedAttrId;
		this.instanceId = instanceId;
	}

	public String getResolvedAttrId() {
		return resolvedAttrId;
	}

	public String getInstanceId() {
		return instanceId;
	}

}
