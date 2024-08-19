package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.Map;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;

public class UpdateAttrHistoryEntityRequest extends BaseRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4591627988809549757L;

	public UpdateAttrHistoryEntityRequest(String tenant, String entityId, String attrId, String instanceId,
			Map<String, Object> payload, boolean zipped) {
		super(tenant, entityId, payload, AppConstants.UPDATE_TEMPORAL_INSTANCE_REQUEST, zipped);
		this.instanceId = instanceId;
		this.attribName = attrId;
	}

}
