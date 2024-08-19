package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class BatchRequest extends BaseRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3640664052782539124L;

	public BatchRequest(String tenant, Set<String> ids, Map<String, List<Map<String, Object>>> payload, int requestType,
			boolean zipped) {
		super(tenant, ids, payload, requestType, zipped);
	}

	

}
