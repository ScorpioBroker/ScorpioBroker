package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.Map;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;

public class CreateHistoryEntityRequest extends BaseRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6553802452235074090L;

	public CreateHistoryEntityRequest(String tenant, Map<String, Object> resolved, boolean zipped) {
		super(tenant, (String) resolved.get(NGSIConstants.JSON_LD_ID), resolved, AppConstants.CREATE_TEMPORAL_REQUEST,
				zipped);
	}

	public CreateHistoryEntityRequest(BaseRequest message) {
		super(message.getTenant(), message.getIds(), message.getPayload(), AppConstants.CREATE_TEMPORAL_REQUEST,
				message.isZipped());
	}

}
