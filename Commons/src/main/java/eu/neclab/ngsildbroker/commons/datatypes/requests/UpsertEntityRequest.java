package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.Map;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;

public class UpsertEntityRequest extends EntityRequest {

	/**
	 * constructor for serialization
	 */
	public UpsertEntityRequest() {

	}

	public UpsertEntityRequest(String tenant, Map<String, Object> resolved) {
		super(tenant, (String) resolved.get(NGSIConstants.JSON_LD_ID), resolved,
				AppConstants.UPSERT_REQUEST);

	}

}
