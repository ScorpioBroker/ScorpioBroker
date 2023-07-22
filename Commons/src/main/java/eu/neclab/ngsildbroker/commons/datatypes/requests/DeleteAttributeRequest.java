package eu.neclab.ngsildbroker.commons.datatypes.requests;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;

public class DeleteAttributeRequest extends EntityRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7154593675061044742L;

	public DeleteAttributeRequest() {

	}

	public DeleteAttributeRequest(String tenant, String entityId, String attribName, String datasetId,
			boolean deleteAll) {
		super(tenant, entityId, null, new BatchInfo(-1, -1), AppConstants.DELETE_ATTRIBUTE_REQUEST);
		this.attribName = attribName;
		this.datasetId = datasetId;
		this.deleteAll = deleteAll;
	}


}
