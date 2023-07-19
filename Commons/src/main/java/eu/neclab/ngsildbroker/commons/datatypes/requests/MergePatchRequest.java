package eu.neclab.ngsildbroker.commons.datatypes.requests;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import java.util.Map;

public class MergePatchRequest extends EntityRequest {

	public MergePatchRequest(String tenant, String id, Map<String, Object> payload) {
		super(tenant, id, payload, AppConstants.MERGE_PATCH_REQUEST);
	}

}
