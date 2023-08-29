package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.Map;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;

public class CreateEntityRequest extends EntityRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8527973760666421025L;

	/**
	 * constructor for serialization
	 */
	public CreateEntityRequest() {

	}

	public CreateEntityRequest(String tenant, Map<String, Object> resolved) {
		super(tenant, (String) resolved.get(NGSIConstants.JSON_LD_ID), resolved, AppConstants.CREATE_REQUEST);

	}

}
