package eu.neclab.ngsildbroker.entityhandler.validationutil;

import java.net.URI;

import javax.servlet.http.HttpServletRequest;

import org.springframework.stereotype.Component;

import eu.neclab.ngsildbroker.commons.datatypes.Entity;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

@Component
public class IdValidationRule implements ValidationRules {

	@Override
	public boolean validateEntity(Entity entity, HttpServletRequest request) throws ResponseException {
		isValidURI(entity.getId().toString());
		return true;
	}

	//check for whether id is a valid URI
	private boolean isValidURI(String urlString) throws ResponseException {
		try {
			new URI(urlString);
			if(!urlString.contains(":")) {
				throw new ResponseException(ErrorType.BadRequestData,"id is not a URI");
			}
			return true;
		} catch (Exception e) {
			throw new ResponseException(ErrorType.BadRequestData,"id is not a URI");
		}
	}

}
