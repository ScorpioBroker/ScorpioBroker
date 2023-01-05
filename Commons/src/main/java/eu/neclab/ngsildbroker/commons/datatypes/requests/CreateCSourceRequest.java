package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.Map;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public class CreateCSourceRequest extends CSourceRequest {

	public CreateCSourceRequest(String tenant, Map<String, Object> resolved) {
		super(tenant, (String) resolved.get(NGSIConstants.JSON_LD_ID), addSysAttrs(resolved), new BatchInfo(-1, -1),
				AppConstants.CREATE_REQUEST);
	}

}
