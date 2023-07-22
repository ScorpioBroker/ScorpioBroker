package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.Map;

import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;

public class HistoryEntityRequest extends EntityRequest {
	/**
	 * 
	 */
	private static final long serialVersionUID = -8680201748125085714L;

	HistoryEntityRequest(String tenant, String id, Map<String, Object> requestPayload, BatchInfo batchInfo,
			int requestType) {
		super(tenant, id, requestPayload, batchInfo, requestType);
	}
}
