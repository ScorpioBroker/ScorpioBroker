package eu.neclab.ngsildbroker.commons.datatypes;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;

public class CSourceRequest extends BaseRequest {
	public CSourceRequest(ArrayListMultimap<String, String> headers, String id, Map<String, Object> requestPayload) {
		super(headers, id, requestPayload);
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
			logger.error(e.getMessage());
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
			logger.error(e.getMessage());
			// should never happen
			return null;
		}
	}

}
