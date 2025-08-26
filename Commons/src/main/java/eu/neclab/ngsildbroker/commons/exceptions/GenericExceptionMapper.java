package eu.neclab.ngsildbroker.commons.exceptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.neclab.ngsildbroker.commons.enums.ErrorType;

import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

@Provider
public class GenericExceptionMapper implements ExceptionMapper<Exception> {
	private static final Logger logger = LoggerFactory.getLogger(GenericExceptionMapper.class);

	@Override
	public Response toResponse(Exception exception) {
		logger.debug("Logs to check", exception);
		return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
				.entity(new ResponseException(ErrorType.InternalError,
						"Something unforseen went wrong check the logs.").getJson())
				.header("Content-Type", "application/json").build();
	}

}
