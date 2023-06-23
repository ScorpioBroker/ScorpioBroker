package eu.neclab.ngsildbroker.commons.datatypes.requests;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;

import java.util.Map;

public class UpdateEntityRequest extends EntityRequest {

	private String attrName;

//	private Map<String,Object> previousEntity;

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

//	public void setPreviousEntity(Map<String,Object> previousEntity){
//		this.previousEntity = previousEntity;
//	}
//	public Map<String,Object> getPreviousEntity(){
//		return previousEntity;
//	}
}
