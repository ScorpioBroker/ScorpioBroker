package eu.neclab.ngsildbroker.commons.exceptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import jakarta.ws.rs.NotSupportedException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

@Provider
public class NotSupportedExceptionMapper implements ExceptionMapper<NotSupportedException> {

	
	@Override
	public Response toResponse(NotSupportedException exception) {

		return Response.status(Response.Status.UNSUPPORTED_MEDIA_TYPE)
				.entity(new ResponseException(ErrorType.UnsupportedMediaType).getJson())
				.header("Content-Type", "application/json").build();
	}

}
