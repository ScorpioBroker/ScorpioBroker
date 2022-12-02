package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.Map;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;

public class DeleteAttributeRequest extends EntityRequest {

	private String attribName;
	private String datasetId;
	private boolean deleteAll;

	public DeleteAttributeRequest() {

	}

	public DeleteAttributeRequest(ArrayListMultimap<String, String> headers, String entityId, String attribName,
			String datasetId, boolean deleteAll) {
		super(headers, entityId, null, new BatchInfo(-1, -1), AppConstants.DELETE_ATTRIBUTE_REQUEST);
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

}
