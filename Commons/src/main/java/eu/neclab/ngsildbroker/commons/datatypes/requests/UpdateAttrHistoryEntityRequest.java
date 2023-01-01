package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.Map;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;

public class UpdateAttrHistoryEntityRequest extends HistoryEntityRequest {

	private String instanceId;
	private String attrId;

	public UpdateAttrHistoryEntityRequest(String tenant, String entityId, String attrId, String instanceId,
			Map<String, Object> payload, BatchInfo batchInfo) {
		super(tenant, entityId, payload, batchInfo, AppConstants.UPDATE_TEMPORAL_INSTANCE_REQUEST);
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
