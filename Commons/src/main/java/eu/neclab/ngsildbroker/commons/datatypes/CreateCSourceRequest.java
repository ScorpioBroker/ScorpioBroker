package eu.neclab.ngsildbroker.commons.datatypes;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaOps;

public class CreateCSourceRequest extends CSourceRequest {

	@Autowired
	@Qualifier("rmops")
	KafkaOps operations;
	@Value("${csource.source.topic}")
	String CSOURCE_TOPIC;

	/**
	 * constructor for serialization
	 */
	public CreateCSourceRequest() {
		super(null, null);
	}

	public CreateCSourceRequest(CSourceRegistration csourceRegistration, ArrayListMultimap<String, String> headers)
			throws ResponseException {

		super(null, headers);
		this.csourceRegistration = csourceRegistration;
		generatePayloadVersions(csourceRegistration);
	}

	private void generatePayloadVersions(CSourceRegistration csourceRegistration) throws ResponseException {

		// CSourceRegistration csourceRegistration =
		// DataSerializer.getCSourceRegistration(payload);
		URI idUri = csourceRegistration.getId();
		if (idUri == null) {
			idUri = generateUniqueRegId(csourceRegistration);
			csourceRegistration.setId(idUri);

		}
		id = idUri.toString();

		if (csourceRegistration.getType() == null) {
			logger.error("Invalid type!");
			throw new ResponseException(ErrorType.BadRequestData);
		}
		if (csourceRegistration.getEndpoint() != null) {
			if (!isValidURL(csourceRegistration.getEndpoint().toString())) {
				logger.error("Invalid endpoint URL!");
				throw new ResponseException(ErrorType.BadRequestData);
			}
		}
		if (csourceRegistration.getInformation() == null) {
			logger.error("Information is empty!");
			throw new ResponseException(ErrorType.BadRequestData);
		}
		if (csourceRegistration.getExpires() != null && !isValidFutureDate(csourceRegistration.getExpires())) {
			logger.error("Invalid expire date!");
			throw new ResponseException(ErrorType.BadRequestData);
		}
	}

	private static boolean isValidURL(String urlString) {
		URL url;
		try {
			url = new URL(urlString);
			url.toURI();
			return true;
		} catch (Exception e) {
			// put logger
		}
		return false;
	}

	// return true for future date validation
	private boolean isValidFutureDate(Long date) {

		return System.currentTimeMillis() < date;
	}

	private URI generateUniqueRegId(CSourceRegistration csourceRegistration) {

		try {

			String key = "urn:ngsi-ld:csourceregistration:" + csourceRegistration.hashCode();
			while (this.operations.isMessageExists(key, this.CSOURCE_TOPIC)) {
				key = key + "1";
			}
			return new URI(key);
		} catch (URISyntaxException e) {
			// Left empty intentionally should never happen
			throw new AssertionError();
		}
	}

}
