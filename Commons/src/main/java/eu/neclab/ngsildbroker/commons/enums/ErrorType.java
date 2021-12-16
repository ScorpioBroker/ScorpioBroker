package eu.neclab.ngsildbroker.commons.enums;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:22
 */
public enum ErrorType {

	None(200, "none", "none"),
	NotAcceptable(406, "https://uri.etsi.org/ngsi-ld/errors/NotAcceptable", "Not an acceptable request."),
	InvalidRequest(400, "https://uri.etsi.org/ngsi-ld/errors/InvalidRequest", "Invalid request."),
	BadRequestData(400, "https://uri.etsi.org/ngsi-ld/errors/BadRequestData", "Bad Request Data."),
	TooComplexQuery(403, "https://uri.etsi.org/ngsi-ld/errors/TooComplexQuery", "Too complex query"),
	TooManyResults(403, "https://uri.etsi.org/ngsi-ld/errors/TooManyResults ", "Too many results"),
	NotFound(404, "https://uri.etsi.org/ngsi-ld/errors/ResourceNotFound", "Resource not found."),
	TenantNotFound(404, "https://uri.etsi.org/ngsi-ld/errors/TenantNotFound", "Tenant not found."),
	LdContextNotAvailable(503, "https://uri.etsi.org/ngsi-ld/errors/LdContextNotAvailable", "LD context not available."),
	AlreadyExists(409, "https://uri.etsi.org/ngsi-ld/errors/AlreadyExists", "Already exists."),
	LenghtRequired(411, "HTTP request provided by a client does not define the “Content-Length” HTTP header",
			"HTTP request provided by a client does not define the “Content-Length” HTTP header"),
	RequestEntityTooLarge(413, "HTTP input data stream is too large i.e. too many bytes",
			"HTTP input data stream is too large i.e. too many bytes"),
	UnsupportedMediaType(415, "Unsupported Media type", "Unsupported Media type"),
	OperationNotSupported(422, "https://uri.etsi.org/ngsi-ld/errors/OperationNotSupported", "Operation not supported."),
	UnprocessableEntity(422, "Unprocessable Entity.", "Unprocessable Entity."),

	InternalError(500, "https://uri.etsi.org/ngsi-ld/errors/InternalError", "Internal error"),
	KafkaWriteError(500, "https://uri.etsi.org/ngsi-ld/errors/InternalError", "Kafka write exception."),
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