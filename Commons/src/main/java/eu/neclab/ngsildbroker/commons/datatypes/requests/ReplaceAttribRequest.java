package eu.neclab.ngsildbroker.commons.datatypes.requests;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;


import java.util.Map;

public class ReplaceAttribRequest extends BaseRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6257331818411623405L;

	/**
	 * constructor for serialization
	 */


	public ReplaceAttribRequest() {

	}

	public ReplaceAttribRequest(String tenant, Map<String, Object> resolved  ,String entityId,String attrId, boolean zipped) {
		super(tenant,entityId, resolved,
				AppConstants.REPLACE_ATTRIBUTE_REQUEST, zipped);
		this.attribName=attrId;
   }

}
