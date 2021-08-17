package eu.neclab.ngsildbroker.commons.tools;

import java.net.URI;
import java.net.URISyntaxException;

import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public class ValidateURI {
	public static String validateUri(String mapValue) throws ResponseException {
		try {
			if (!new URI(mapValue).isAbsolute()) {
				throw new ResponseException(ErrorType.BadRequestData, "id is not a URI");
			}
			return mapValue;
		} catch (URISyntaxException e) {
			throw new ResponseException(ErrorType.BadRequestData, "id is not a URI");
		}

	}
}
