package eu.neclab.ngsildbroker.commons.exceptions;

import com.fasterxml.jackson.core.JsonProcessingException;

import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

@Provider
public class JsonProcessingExceptionMapper implements ExceptionMapper<JsonProcessingException> {
	
	
	@Override
	public Response toResponse(JsonProcessingException exception) {
		
		return Response.status(Response.Status.BAD_REQUEST)
				.entity(new ResponseException(ErrorType.InvalidRequest,
						"There is an error in the provided json document").getJson())
				.header("Content-Type", "application/json").build();
	}

}
