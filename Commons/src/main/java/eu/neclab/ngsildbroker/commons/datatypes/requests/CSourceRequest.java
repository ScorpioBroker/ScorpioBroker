package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.Map;
import com.google.common.collect.ArrayListMultimap;

import io.vertx.core.json.JsonObject;

public class CSourceRequest extends BaseRequest {
	CSourceRequest(String tenant, String id, Map<String, Object> requestPayload,
			int requestType) {
		super(tenant, id, requestPayload, requestType);
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
