package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.io.IOException;
import java.util.Map;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;

import io.vertx.core.json.JsonObject;

public class CSourceRequest extends BaseRequest {
	CSourceRequest(ArrayListMultimap<String, String> headers, String id, Map<String, Object> requestPayload,
			int requestType) {
		super(headers, id, requestPayload, requestType);
	}

	public CSourceRequest() {
	}

	public JsonObject getResultCSourceRegistrationString() {
		if (finalPayload == null) {
			return JsonObject.mapFrom(null);
		}
		return JsonObject.mapFrom(finalPayload);

	}

	public JsonObject getOperationCSourceRegistrationString() {
		if (requestPayload == null) {
			return JsonObject.mapFrom(null);
		}
		return JsonObject.mapFrom(requestPayload);

	}

}
