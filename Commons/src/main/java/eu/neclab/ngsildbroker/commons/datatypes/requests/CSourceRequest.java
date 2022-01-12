package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.io.IOException;
import java.util.Map;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;

public class CSourceRequest extends BaseRequest {
	CSourceRequest(ArrayListMultimap<String, String> headers, String id, Map<String, Object> requestPayload,
			int requestType) {
		super(headers, id, requestPayload, requestType);
	}

	public CSourceRequest() {
	}

	public String getResultCSourceRegistrationString() {
		if (finalPayload == null) {
			return null;
		}
		try {
			return JsonUtils.toString(finalPayload);
		} catch (IOException e) {
			// should never happen
			return null;
		}
	}

	public String getOperationCSourceRegistrationString() {
		if (requestPayload == null) {
			return null;
		}
		try {
			return JsonUtils.toString(requestPayload);
		} catch (IOException e) {
			// should never happen
			return null;
		}
	}

}
