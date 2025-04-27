package eu.neclab.ngsildbroker.commons.exceptions;




import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

@Provider
public class GenericExceptionMapper implements ExceptionMapper<Exception> {

	
	@Override
	public Response toResponse(Exception exception) {
		return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
				.entity(new ResponseException(ErrorType.InternalError,
						"Something unforseen went wrong check the logs.").getJson())
				.header("Content-Type", "application/json").build();
	}

}
