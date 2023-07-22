package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.Map;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;

public class UpsertEntityRequest extends EntityRequest {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4685549934770083725L;

	/**
	 * constructor for serialization
	 */
	public UpsertEntityRequest() {

	}

	public UpsertEntityRequest(String tenant, Map<String, Object> resolved, BatchInfo batchInfo) {
		super(tenant, (String) resolved.get(NGSIConstants.JSON_LD_ID), resolved, batchInfo,
				AppConstants.UPSERT_REQUEST);

	}

}
