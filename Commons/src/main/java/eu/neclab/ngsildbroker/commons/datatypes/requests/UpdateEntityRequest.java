package eu.neclab.ngsildbroker.commons.datatypes.requests;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;

import java.util.Map;

public class UpdateEntityRequest extends EntityRequest {

	

	/**
	 * 
	 */
	private static final long serialVersionUID = -4626344157475548782L;

	public UpdateEntityRequest(String tenant, String id, Map<String, Object> payload, String attrName,
                               BatchInfo batchInfo) {
		super(tenant, id, payload, batchInfo, AppConstants.UPDATE_REQUEST);
		this.attribName = attrName;
	}

	
}
