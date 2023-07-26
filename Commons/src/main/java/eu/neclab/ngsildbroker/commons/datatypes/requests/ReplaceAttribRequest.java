package eu.neclab.ngsildbroker.commons.datatypes.requests;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;


import java.util.Map;

public class ReplaceAttribRequest extends EntityRequest {

	/**
	 * constructor for serialization
	 */


	public ReplaceAttribRequest() {

	}

	public ReplaceAttribRequest(String tenant, Map<String, Object> resolved  ,String entityId,String attrId) {
		super(tenant,entityId, resolved,
				AppConstants.REPLACE_ATTRIBUTE_REQUEST);
		this.attribName=attrId;
   }

}
