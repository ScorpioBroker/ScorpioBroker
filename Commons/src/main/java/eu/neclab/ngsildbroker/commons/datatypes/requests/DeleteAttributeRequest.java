package eu.neclab.ngsildbroker.commons.datatypes.requests;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;

public class DeleteAttributeRequest extends EntityRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5728664809196171935L;

	public DeleteAttributeRequest() {

	}

	public DeleteAttributeRequest(String tenant, String entityId, String attribName, String datasetId,
			boolean deleteAll) {
		super(tenant, entityId, null, AppConstants.DELETE_ATTRIBUTE_REQUEST);
		this.attribName = attribName;
		this.datasetId = datasetId;
		this.deleteAll = deleteAll;
	}

	
}
