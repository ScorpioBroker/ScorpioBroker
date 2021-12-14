package eu.neclab.ngsildbroker.commons.datatypes;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public class AppendCSourceRequest extends CSourceRequest {

	public AppendCSourceRequest(ArrayListMultimap<String, String> headers, String registrationId,
			JsonNode entityJsonBody, CSourceRegistration csourceRegistration) throws ResponseException {
		super(null, headers);
		this.csourceRegistration = csourceRegistration;
		this.id=registrationId;

	}

}
