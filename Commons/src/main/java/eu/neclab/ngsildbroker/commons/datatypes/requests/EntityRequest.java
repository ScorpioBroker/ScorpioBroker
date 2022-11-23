package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;
import io.vertx.core.json.JsonObject;

public class EntityRequest extends BaseRequest {

	public EntityRequest() {

	}

	EntityRequest(ArrayListMultimap<String, String> headers,  Map<String, Object> requestPayload, BatchInfo batchInfo,
			int requestType) {
		super(headers, requestPayload, batchInfo, requestType);
	}


}
