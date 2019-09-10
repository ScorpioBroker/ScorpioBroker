package eu.neclab.ngsildbroker.entityhandler.validationutil;

import java.net.URI;

import javax.servlet.http.HttpServletRequest;

import org.springframework.stereotype.Component;

import eu.neclab.ngsildbroker.commons.datatypes.Entity;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

@Component
public class TypeValidationRule implements ValidationRules{
	//TODO log or remove
//	 private final static Logger logger = LogManager.getLogger(TypeValidationRule.class);
	
	//should reject an entity which node type is not recognized OR null
	@Override
	public boolean validateEntity(Entity entity,HttpServletRequest request) throws ResponseException {
		isTypeRecognized(entity.getType());
		return true;
	}

	private boolean isTypeRecognized(String type) throws ResponseException{
		try {
			new URI(type);
			if(!type.contains(":")) {
				throw new ResponseException(ErrorType.BadRequestData,"@type is not recognized");
			}
			return true;
		} catch (Exception e) {
			throw new ResponseException(ErrorType.BadRequestData,"@type is not recognized");
		}
	}

}
