package eu.neclab.ngsildbroker.commons.exceptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.fasterxml.jackson.core.JsonProcessingException;

import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

@Provider
public class JsonProcessingExceptionMapper implements ExceptionMapper<JsonProcessingException> {
	private static Logger logger = LoggerFactory.getLogger(JsonProcessingExceptionMapper.class);
	
	@Override
	public Response toResponse(JsonProcessingException exception) {
		logger.debug("failed to process JSON.", exception);
		return Response.status(Response.Status.BAD_REQUEST)
				.entity(new ResponseException(ErrorType.InvalidRequest,
						"There is an error in the provided json document").getJson())
				.header("Content-Type", "application/json").build();
	}

}
