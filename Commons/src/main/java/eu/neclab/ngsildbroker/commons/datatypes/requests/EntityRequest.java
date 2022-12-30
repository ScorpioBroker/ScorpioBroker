package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.time.Instant;
import java.util.Map;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;

public class EntityRequest extends BaseRequest {

	public EntityRequest() {

	}

	EntityRequest(String tenant, String id, Map<String, Object> requestPayload, BatchInfo batchInfo, int requestType) {
		super(tenant, id, requestPayload, batchInfo, requestType);
		addSysAttrs(requestPayload);
	}

	public static Map<String, Object> addSysAttrs(Map<String, Object> resolved) {
		String now = SerializationTools.formatter.format(Instant.now());
		setTemporalProperties(resolved, now, now, false);
		return resolved;
	}

}
