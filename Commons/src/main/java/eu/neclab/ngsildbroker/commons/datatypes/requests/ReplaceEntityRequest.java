package eu.neclab.ngsildbroker.commons.datatypes.requests;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;


import java.util.Map;

public class ReplaceEntityRequest extends EntityRequest {

	/**
	 * constructor for serialization
	 */
	public ReplaceEntityRequest() {

	}

	public ReplaceEntityRequest(String tenant, Map<String, Object> resolved ) {
		super(tenant, (String) resolved.get(NGSIConstants.JSON_LD_ID), resolved,
				AppConstants.REPLACE_ENTITY_REQUEST);

	}

}
