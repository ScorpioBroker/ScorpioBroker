package eu.neclab.ngsildbroker.commons.datatypes.requests;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;

import java.util.HashMap;
import java.util.Map;

public class ReplaceAttribRequest extends EntityRequest {

	/**
	 * constructor for serialization
	 */


	public ReplaceAttribRequest() {

	}

	public ReplaceAttribRequest(String tenant, Map<String, Object> resolved, BatchInfo batchInfo,String entityId,String attrId) {
		super(tenant,entityId, resolved, batchInfo,
				AppConstants.CREATE_REQUEST);
		this.attribName=attrId;

	}

}
