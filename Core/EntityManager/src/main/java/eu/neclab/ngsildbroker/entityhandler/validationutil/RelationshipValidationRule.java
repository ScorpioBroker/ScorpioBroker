package eu.neclab.ngsildbroker.entityhandler.validationutil;

import javax.servlet.http.HttpServletRequest;

import org.springframework.stereotype.Component;

import eu.neclab.ngsildbroker.commons.datatypes.Entity;
import eu.neclab.ngsildbroker.commons.datatypes.Relationship;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

@Component
public class RelationshipValidationRule implements ValidationRules{

	@Override
	public boolean validateEntity(Entity entity,HttpServletRequest request) throws ResponseException {
		isNullObject(entity);
		isEmptyObject(entity);
		return true;
	}
	
	private boolean isNullObject(Entity entity) throws ResponseException {
		for(Relationship relation:entity.getRelationships()) {
			if(relation.getEntries()==null) {
				throw new ResponseException(ErrorType.BadRequestData,"Entity with a Relationship object equal to null");
			}
		}
		return true;
	}
	
	private boolean isEmptyObject(Entity entity) {
		return true;
	}

}
