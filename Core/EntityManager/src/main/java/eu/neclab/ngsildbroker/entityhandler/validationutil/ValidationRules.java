package eu.neclab.ngsildbroker.entityhandler.validationutil;

import javax.servlet.http.HttpServletRequest;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.neclab.ngsildbroker.commons.datatypes.Entity;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public interface ValidationRules {
	static final ObjectMapper objectMapper = new ObjectMapper();
	public boolean validateEntity(Entity entity,HttpServletRequest request) throws ResponseException;
}
