package eu.neclab.ngsildbroker.entityhandler.validationutil;

import javax.servlet.http.HttpServletRequest;

import org.springframework.stereotype.Component;

import eu.neclab.ngsildbroker.commons.datatypes.Entity;
import eu.neclab.ngsildbroker.commons.datatypes.Property;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

@Component
public class PropertyValidatioRule implements ValidationRules {


	@Override
	public boolean validateEntity(Entity entity, HttpServletRequest request) throws ResponseException {
		isNullProperty(entity);
		return true;
	}
	
	private boolean isNullProperty(Entity entity) throws ResponseException {
		for(Property property:entity.getProperties()) {
			if(property.getEntries()==null) {
				throw new ResponseException(ErrorType.BadRequestData,"Entity with a property value equal to null");
			}
		}
		return true;
	}
	
	@SuppressWarnings("unused")
	//TODO use or remove
	private boolean isEmptyProperty(Entity entity) {
		return true;
	}
	
	

}
