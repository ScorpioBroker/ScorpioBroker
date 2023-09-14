package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.Map;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;

public class CreateHistoryEntityRequest extends HistoryEntityRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6553802452235074090L;

	public CreateHistoryEntityRequest(String tenant, Map<String, Object> resolved) {
		super(tenant, (String) resolved.get(NGSIConstants.JSON_LD_ID), resolved, 
				AppConstants.CREATE_TEMPORAL_REQUEST);
	}

	public CreateHistoryEntityRequest(BaseRequest message) {
		this(message.getTenant(), message.getPayload());
	}

}
