package eu.neclab.ngsildbroker.commons.datatypes;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaOps;

public class DeleteCSourceRequest extends CSourceRequest {

	@Autowired
	@Qualifier("rmops")
	KafkaOps operations;
	@Value("${csource.source.topic}")
	String CSOURCE_TOPIC;

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
