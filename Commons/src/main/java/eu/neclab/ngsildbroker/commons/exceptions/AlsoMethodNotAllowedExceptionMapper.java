package eu.neclab.ngsildbroker.commons.exceptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import jakarta.ws.rs.NotAllowedException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

@Provider
public class AlsoMethodNotAllowedExceptionMapper implements ExceptionMapper<NotAllowedException> {
	private static final Logger logger = LoggerFactory.getLogger(AlsoMethodNotAllowedExceptionMapper.class);

	@Override
	public Response toResponse(NotAllowedException exception) {
		logger.debug("Logs to check", exception);
		return Response.status(Response.Status.METHOD_NOT_ALLOWED)
				.entity(new ResponseException(ErrorType.MethodNotAllowed).getJson())
				.header("Content-Type", "application/json").build();
	}

}
