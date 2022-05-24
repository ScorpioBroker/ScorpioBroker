package eu.neclab.ngsildbroker.commons.exceptions;

import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * @version 1.0
 * @created 09-Jul-2018
 */
public class ResponseException extends Exception {
	private static final long serialVersionUID = 1L;

	private HttpResponseStatus httpStatus;
	private ErrorType error;

	public ResponseException(ErrorType error, String errorMessage) {
		super(errorMessage);
		this.error = error;
		this.httpStatus = HttpResponseStatus.valueOf(error.getCode());
	}

	public HttpResponseStatus getHttpStatus() {
		return httpStatus;
	}

	public ErrorType getError() {
		return error;
	}

	@Override
	public String toString() {
		return super.getMessage();
	}

}
