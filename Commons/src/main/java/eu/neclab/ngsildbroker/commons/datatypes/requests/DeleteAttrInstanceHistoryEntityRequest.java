package eu.neclab.ngsildbroker.commons.datatypes.requests;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;

public class DeleteAttrInstanceHistoryEntityRequest extends HistoryEntityRequest {

	private String instanceId;
	private String attrId;

	public DeleteAttrInstanceHistoryEntityRequest(String tenant, String entityId, String attrId, String instanceId,
			BatchInfo batchInfo) {
		super(tenant, entityId, null, batchInfo, AppConstants.DELETE_TEMPORAL_ATTRIBUTE_REQUEST);
		this.instanceId = instanceId;
		this.attrId = attrId;
	}

	public String getInstanceId() {
		return instanceId;
	}

	public String getAttrId() {
		return attrId;
	}

}
