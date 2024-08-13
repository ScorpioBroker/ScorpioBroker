package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.Map;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;

public class UpsertEntityRequest extends BaseRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3200833211917228827L;

	/**
	 * constructor for serialization
	 */
	public UpsertEntityRequest() {

	}

	public UpsertEntityRequest(String tenant, Map<String, Object> resolved, boolean zipped) {
		super(tenant, (String) resolved.get(NGSIConstants.JSON_LD_ID), resolved,
				AppConstants.UPSERT_REQUEST, zipped);

	}

}
