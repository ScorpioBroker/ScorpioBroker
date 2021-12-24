package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.Map;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public class CreateCSourceRequest extends CSourceRequest {

	
	public CreateCSourceRequest(Map<String, Object> resolved, ArrayListMultimap<String, String> headers, String id)
			throws ResponseException {
		super(headers, id, resolved);
		setFinalPayload(resolved);
	}

	

	

}
