package eu.neclab.ngsildbroker.commons.datatypes.requests;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;

public class DeleteAttrInstanceHistoryEntityRequest extends BaseRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5668055593226987622L;
	
	public DeleteAttrInstanceHistoryEntityRequest(String tenant, String entityId, String attrId, String instanceId, boolean zipped) {
		super(tenant, entityId, null, AppConstants.DELETE_TEMPORAL_ATTRIBUTE_REQUEST, zipped);
		this.instanceId = instanceId;
		this.attribName = attrId;
	}

	

}
