package eu.neclab.ngsildbroker.commons.datatypes;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public class DeleteCSourceRequest extends CSourceRequest {

	

	/**
	 * constructor for serialization
	 */
	public DeleteCSourceRequest() {
		super(null, null);
	}

	public DeleteCSourceRequest(CSourceRegistration csourceRegistration, ArrayListMultimap<String, String> headers,
			String registrationId) throws ResponseException {

		super(null, headers);
		this.csourceRegistration = csourceRegistration;
		this.id = registrationId;

	}

}
