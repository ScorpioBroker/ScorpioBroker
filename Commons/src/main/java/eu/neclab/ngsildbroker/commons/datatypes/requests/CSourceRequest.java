package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.Map;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;

public class CSourceRequest extends BaseRequest {
	CSourceRequest(ArrayListMultimap<String, String> headers, String id, Map<String, Object> requestPayload,
			BatchInfo batchInfo, int requestType) {
		super(headers, id, requestPayload, batchInfo, requestType);
	}

	public CSourceRequest() {
	}

}
