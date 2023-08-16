package eu.neclab.ngsildbroker.commons.datatypes.requests;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import java.util.Map;

public class UpdateEntityRequest extends EntityRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2142446723256043172L;

	public UpdateEntityRequest(String tenant, String id, Map<String, Object> payload, String attrName) {
		super(tenant, id, payload, AppConstants.UPDATE_REQUEST);
		this.attribName = attrName;
	}
}
