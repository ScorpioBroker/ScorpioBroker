package eu.neclab.ngsildbroker.commons.datatypes.requests;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;

public class DeleteAttrInstanceHistoryEntityRequest extends HistoryEntityRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5668055593226987622L;
	private String instanceId;
	

	public DeleteAttrInstanceHistoryEntityRequest(String tenant, String entityId, String attrId, String instanceId) {
		super(tenant, entityId, null, AppConstants.DELETE_TEMPORAL_ATTRIBUTE_REQUEST);
		this.instanceId = instanceId;
		this.attribName = attrId;
	}

	public String getInstanceId() {
		return instanceId;
	}

	

}
