package eu.neclab.ngsildbroker.commons.datatypes.requests;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;

public class DeleteAttributeRequest extends BaseRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7154593675061044742L;

	public DeleteAttributeRequest() {

	}

	public DeleteAttributeRequest(String tenant, String entityId, String attribName, String datasetId,
			boolean deleteAll, boolean zipped) {
		super(tenant, entityId, null, AppConstants.DELETE_ATTRIBUTE_REQUEST, zipped);
		this.attribName = attribName;
		this.datasetId = datasetId;
		this.deleteAll = deleteAll;
	}

}
