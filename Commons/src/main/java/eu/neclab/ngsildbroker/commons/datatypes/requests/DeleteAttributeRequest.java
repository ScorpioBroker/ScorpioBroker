package eu.neclab.ngsildbroker.commons.datatypes.requests;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;

public class DeleteAttributeRequest extends EntityRequest {

	public DeleteAttributeRequest() {

	}

	public DeleteAttributeRequest(String tenant, String entityId, String attribName, String datasetId,
			boolean deleteAll) {
		super(tenant, entityId, null, AppConstants.DELETE_ATTRIBUTE_REQUEST);
		this.attribName = attribName;
		this.datasetId = datasetId;
		this.deleteAll = deleteAll;
	}

	public String getAttribName() {
		return attribName;
	}

	public String getDatasetId() {
		return datasetId;
	}

	public boolean deleteAll() {
		return deleteAll;
	}

	public boolean isDeleteAll() {
		return deleteAll;
	}

	public void setDeleteAll(boolean deleteAll) {
		this.deleteAll = deleteAll;
	}

	public void setAttribName(String attribName) {
		this.attribName = attribName;
	}

	public void setDatasetId(String datasetId) {
		this.datasetId = datasetId;
	}

}
