package eu.neclab.ngsildbroker.commons.datatypes.requests;

import java.util.Map;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import io.vertx.core.MultiMap;

public class CreateCSourceRequest extends CSourceRequest {

	public CreateCSourceRequest(Map<String, Object> resolved, MultiMap headers, String id) throws ResponseException {
		super(headers, id, resolved, AppConstants.CREATE_REQUEST);
		setFinalPayload(resolved);
	}

}
