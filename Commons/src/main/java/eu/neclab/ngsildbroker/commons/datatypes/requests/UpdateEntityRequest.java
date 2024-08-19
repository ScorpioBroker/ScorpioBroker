package eu.neclab.ngsildbroker.commons.datatypes.requests;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import java.util.Map;

public class UpdateEntityRequest extends BaseRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5787924148453373416L;

	public UpdateEntityRequest(String tenant, String id, Map<String, Object> payload, String attrName, boolean zipped) {
		super(tenant, id, payload, AppConstants.UPDATE_REQUEST, zipped);
		this.attribName = attrName;
	}
}
