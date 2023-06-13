package eu.neclab.ngsildbroker.commons.datatypes.requests;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;

public class DeleteAttrHistoryEntityRequest extends HistoryEntityRequest {

	private boolean deleteAll;
	private String datasetId;
	private String attribName;

	public DeleteAttrHistoryEntityRequest(String tenant, String entityId, String attribName, String datasetId,
			boolean deleteAll, BatchInfo batchInfo) {
		super(tenant, entityId, null, batchInfo, AppConstants.DELETE_TEMPORAL_ATTRIBUTE_REQUEST);
		this.attribName = attribName;
		this.datasetId = datasetId;
		this.deleteAll = deleteAll;
	}

	public boolean isDeleteAll() {
		return deleteAll;
	}

	public String getDatasetId() {
		return datasetId;
	}

	public String getAttribName() {
		return attribName;
	}

}
