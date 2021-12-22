package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.Map;
import java.util.UUID;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public class CreateCSourceRequest extends CSourceRequest {

	public CreateCSourceRequest(Map<String, Object> resolved, ArrayListMultimap<String, String> headers)
			throws ResponseException {
		super(headers);
		this.csourceRegistration = resolved;
		generatePayloadVersions(resolved);
	}

	private void generatePayloadVersions(Map<String, Object> resolved) throws ResponseException {
		Object idObj = resolved.get(NGSIConstants.JSON_LD_ID);
		if (idObj == null) {
			id = generateUniqueRegId();
			resolved.put(NGSIConstants.JSON_LD_ID, id);
		} else {
			id = (String) idObj;
		}
	}

	

}
