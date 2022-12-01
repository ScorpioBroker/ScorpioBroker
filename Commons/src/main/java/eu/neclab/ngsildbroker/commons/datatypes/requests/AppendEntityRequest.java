package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.Map;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;

public class AppendEntityRequest extends EntityRequest {

	public AppendEntityRequest(ArrayListMultimap<String, String> headers, String id, Map<String, Object> payload,
			BatchInfo batchInfo) {
		super(headers, id, payload, batchInfo, AppConstants.UPDATE_REQUEST);

	}
}
