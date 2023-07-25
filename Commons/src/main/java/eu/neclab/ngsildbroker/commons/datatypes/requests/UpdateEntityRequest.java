package eu.neclab.ngsildbroker.commons.datatypes.requests;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import java.util.Map;

public class UpdateEntityRequest extends EntityRequest {

	private String attrName;

	public UpdateEntityRequest(String tenant, String id, Map<String, Object> payload, String attrName) {
		super(tenant, id, payload, AppConstants.UPDATE_REQUEST);
		this.attrName = attrName;
	}

	public String getAttrName() {
		return attrName;
	}

	public void setAttrName(String attrName) {
		this.attrName = attrName;
	}
}
