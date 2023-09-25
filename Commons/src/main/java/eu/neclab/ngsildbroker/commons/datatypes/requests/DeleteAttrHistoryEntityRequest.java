package eu.neclab.ngsildbroker.commons.datatypes.requests;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;

public class DeleteAttrHistoryEntityRequest extends HistoryEntityRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = -5546291614045250694L;
	private boolean deleteAll;

	public DeleteAttrHistoryEntityRequest(String tenant, String entityId, String attribName, String datasetId,
			boolean deleteAll) {
		super(tenant, entityId, null, AppConstants.DELETE_TEMPORAL_ATTRIBUTE_REQUEST);
		this.attribName = attribName;
		this.datasetId = datasetId;
		this.deleteAll = deleteAll;
	}

	public boolean isDeleteAll() {
		return deleteAll;
	}

}
