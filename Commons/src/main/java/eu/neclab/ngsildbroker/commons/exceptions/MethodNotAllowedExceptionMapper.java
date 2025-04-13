package eu.neclab.ngsildbroker.commons.exceptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.NotSupportedException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

@Provider
public class MethodNotAllowedExceptionMapper implements ExceptionMapper<NotFoundException> {
	private static Logger logger = LoggerFactory.getLogger(MethodNotAllowedExceptionMapper.class);
	
	@Override
	public Response toResponse(NotFoundException exception) {
		logger.debug("unsupported media type", exception);
		return Response.status(Response.Status.METHOD_NOT_ALLOWED)
				.entity(new ResponseException(ErrorType.MethodNotAllowed).getJson())
				.header("Content-Type", "application/json").build();
	}

}
