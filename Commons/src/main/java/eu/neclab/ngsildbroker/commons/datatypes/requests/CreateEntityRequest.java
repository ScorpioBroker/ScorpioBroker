package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.time.Instant;
import java.util.Map;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;

public class CreateEntityRequest extends EntityRequest {

	/**
	 * constructor for serialization
	 */
	public CreateEntityRequest() {

	}

	public CreateEntityRequest(Map<String, Object> resolved, ArrayListMultimap<String, String> headers,
			BatchInfo batchInfo) {
		super(headers, addSysAttrs(resolved), batchInfo, AppConstants.CREATE_REQUEST);

	}

	private static Map<String, Object> addSysAttrs(Map<String, Object> resolved) {
		String now = SerializationTools.formatter.format(Instant.now());
		setTemporalProperties(resolved, now, now, false);
		return resolved;
	}

}
