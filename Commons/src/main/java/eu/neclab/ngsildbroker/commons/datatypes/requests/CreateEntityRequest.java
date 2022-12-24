package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.Map;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;

public class CreateEntityRequest extends EntityRequest {

	/**
	 * constructor for serialization
	 */
	public CreateEntityRequest() {

	}

	public CreateEntityRequest(String tenant, Map<String, Object> resolved, BatchInfo batchInfo) {
		super(tenant, (String) resolved.get(NGSIConstants.JSON_LD_ID), addSysAttrs(resolved), batchInfo,
				AppConstants.CREATE_REQUEST);

	}

}
