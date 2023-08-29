package eu.neclab.ngsildbroker.commons.datatypes.requests;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import java.util.Map;

public class MergePatchRequest extends EntityRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1240556031891186239L;

	public MergePatchRequest(String tenant, String id, Map<String, Object> payload) {
		super(tenant, id, payload, AppConstants.MERGE_PATCH_REQUEST);
	}

}
