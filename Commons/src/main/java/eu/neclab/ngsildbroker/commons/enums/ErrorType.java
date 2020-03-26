package eu.neclab.ngsildbroker.commons.enums;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:22
 */
public enum ErrorType {

	None(200, "none", "none"),
	InvalidRequest(406, "http://uri.etsi.org/ngsi-ld/errors/InvalidRequest", "Invalid request."),
	BadRequestData(400, "http://uri.etsi.org/ngsi-ld/errors/BadRequestData", "Bad Request Data."),
	TooComplexQuery(403, "http://uri.etsi.org/ngsi-ld/errors/TooComplexQuery", "Too complex query"),
	TooManyResults(403, "http://uri.etsi.org/ngsi-ld/errors/TooManyResults ", "Too many results"),
	NotFound(404, "http://uri.etsi.org/ngsi-ld/errors/ResourceNotFound", "Resource not found."),

	// ResourceNotFound(404,"Resource not found."),
	// MethodNotAllowed(405,"Method not allowed"),

	AlreadyExists(409, "http://uri.etsi.org/ngsi-ld/errors/AlreadyExists", "Already exists."),
	LenghtRequired(411, "HTTP request provided by a client does not define the “Content-Length” HTTP header",
			"HTTP request provided by a client does not define the “Content-Length” HTTP header"),
	RequestEntityTooLarge(413, "HTTP input data stream is too large i.e. too many bytes",
			"HTTP input data stream is too large i.e. too many bytes"),
	UnsupportedMediaType(415, "Unsupported Media type", "Unsupported Media type"),
	OperationNotSupported(422, "http://uri.etsi.org/ngsi-ld/errors/OperationNotSupported", "Operation not supported."),
	UnprocessableEntity(422, "Unprocessable Entity.", "Unprocessable Entity."),

	InternalError(500, "http://uri.etsi.org/ngsi-ld/errors/InternalError", "Internal error"),
	KafkaWriteError(500, "http://uri.etsi.org/ngsi-ld/errors/InternalError", "Kafka write exception."),
	MultiStatus(207, "Multi status result", "Multi status result");

	private final int code;
	private final String message;
	private String errorType;

	private ErrorType(int code, String errorType, String message) {
		this.code = code;
		this.message = message;
		this.errorType = errorType;
	}

	public int getCode() {
		return code;
	}

	public String getErrorType() {
		return errorType;
	}

	public String getMessage() {
		return message;
	}

	public String toString() {
		return "[" + code + " : " + message + " ]";
	}

}