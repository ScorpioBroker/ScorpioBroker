package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import com.github.jsonldjava.core.Context;

import com.github.jsonldjava.core.JsonLdConsts;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;

public class BatchRequest extends BaseRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3640664052782539124L;

	public BatchRequest(String tenant, Set<String> ids, Map<String, List<Map<String, Object>>> payload, int requestType,
			boolean zipped) {
		super(tenant, ids, payload, requestType, zipped);
	}

	public String getId() {
		return String.join(",", ids);
	}

}
