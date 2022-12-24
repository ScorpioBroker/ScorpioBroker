package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.Map;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;

public class UpdateEntityRequest extends EntityRequest {

	private String attrName;

	public UpdateEntityRequest(String tenant, String id, Map<String, Object> payload, String attrName,
			BatchInfo batchInfo) {
		super(tenant, id, payload, batchInfo, AppConstants.UPDATE_REQUEST);
		this.attrName = attrName;
	}

	public String getAttrName() {
		return attrName;
	}

	public void setAttrName(String attrName) {
		this.attrName = attrName;
	}

}
