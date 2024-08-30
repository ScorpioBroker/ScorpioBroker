package eu.neclab.ngsildbroker.commons.datatypes.requests;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;

public class DeleteAttrHistoryEntityRequest extends BaseRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5546291614045250694L;
	

	public DeleteAttrHistoryEntityRequest(String tenant, String entityId, String attribName, String datasetId,
			boolean deleteAll, boolean zipped) {
		super(tenant, entityId, null, AppConstants.DELETE_TEMPORAL_ATTRIBUTE_REQUEST, zipped);
		this.attribName = attribName;
		this.datasetId = datasetId;
		this.deleteAll = deleteAll;
	}

}
