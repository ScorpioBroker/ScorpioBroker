package eu.neclab.ngsildbroker.commons.exceptions;

import java.util.List;
import java.util.Map;

import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.MultiMap;

/**
 * @version 1.0
 * @created 09-Jul-2018
 */
public class ResponseException extends Exception {
	private static final long serialVersionUID = 1L;

	private HttpResponseStatus httpStatus;
	private ErrorType error;
	private String endpoint;
	private MultiMap headers;

	public ResponseException(ErrorType error) {
		super(error.getMessage());
		this.error = error;
		this.httpStatus = HttpResponseStatus.valueOf(error.getCode());

	}

	public ResponseException(ErrorType error, String errorMessage) {
		super(errorMessage);
		this.error = error;
		this.httpStatus = HttpResponseStatus.valueOf(error.getCode());

	}

	public ResponseException(ErrorType error, String endpoint, MultiMap headers) {
		this(error);
		this.endpoint = endpoint;
		this.headers = headers;

	}

	public ResponseException(ErrorType error, String message, String endpoint, MultiMap headers) {
		this(error, message);
		this.endpoint = endpoint;
		this.headers = headers;
	}

	public HttpResponseStatus getHttpStatus() {
		return httpStatus;
	}

	public ErrorType getError() {
		return error;
	}

	public String getEndpoint() {
		return endpoint;
	}

	public MultiMap getHeaders() {
		return headers;
	}

	@Override
	public String toString() {
		return super.getMessage();
	}

}
