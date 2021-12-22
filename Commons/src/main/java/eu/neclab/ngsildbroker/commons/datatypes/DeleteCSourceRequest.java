package eu.neclab.ngsildbroker.commons.datatypes;

import java.util.Map;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public class DeleteCSourceRequest extends CSourceRequest {

	/**
	 * constructor for serialization
	 */
	public DeleteCSourceRequest() {
		super(null);
	}

	public DeleteCSourceRequest(Map<String, Object> registration, ArrayListMultimap<String, String> headers,
			String registrationId) throws ResponseException {
		super(headers);
		this.csourceRegistration = registration;
		this.id = registrationId;
	}

}
