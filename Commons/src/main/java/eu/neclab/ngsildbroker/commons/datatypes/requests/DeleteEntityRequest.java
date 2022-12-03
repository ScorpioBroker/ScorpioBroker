package eu.neclab.ngsildbroker.commons.datatypes.requests;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;

public class DeleteEntityRequest extends EntityRequest {

	public DeleteEntityRequest() {
	}

	public DeleteEntityRequest(ArrayListMultimap<String, String> headers, String entityId, BatchInfo batchInfo) {
		super(headers, entityId, null, batchInfo, AppConstants.DELETE_REQUEST);
	}

}
