package eu.neclab.ngsildbroker.commons.datatypes.requests;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public class DeleteHistoryEntityRequest extends HistoryEntityRequest {

	private String resolvedAttrId;
	private String instanceId;

	public DeleteHistoryEntityRequest() {
	}

	public DeleteHistoryEntityRequest(ArrayListMultimap<String, String> headers, String resolvedAttrId,
			String instanceId, String entityId) throws ResponseException {
		super(headers, null, entityId, AppConstants.DELETE_REQUEST);
		this.resolvedAttrId = resolvedAttrId;
		this.instanceId = instanceId;
	}

	public String getResolvedAttrId() {
		return resolvedAttrId;
	}

	public String getInstanceId() {
		return instanceId;
	}

}
