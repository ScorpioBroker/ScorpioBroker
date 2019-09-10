package eu.neclab.ngsildbroker.commons.enums;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:13:22
 */
public enum ErrorType {
	
	None(200, "none."),
	
	InvalidRequest(400, "Invalid request."),
	BadRequestData(400, "Bad Request Data."),
	TooComplexQuery(403,"Too complex query"),
	TooManyResults(403,"Too many results"),
	NotFound(404,"Not Found."),
	ResourceNotFound(404,"Resource not found."),
	MethodNotAllowed(405,"Method not allowed"),
	AlreadyExists(409,"Already exists."),
	LenghtRequired(411,"HTTP request provided by a client does not define the “Content-Length” HTTP header"),
	RequestEntityTooLarge(413,"HTTP input data stream is too large i.e. too many bytes"),
	UnsupportedMediaType(415,"Unsupported Media type."),
	OperationNotSupported(422, "Operation not supported."),
	UnprocessableEntity(422, "Unprocessable Entity."),
	
	InternalError(500, "Internal error."),
	KafkaWriteError(500,"Kafka write exception.");
	
	private final int code;
	private final String message;

	private ErrorType(int code, String message) {
	    this.code = code;
	    this.message = message;
	  }

	public int getCode() {
		return code;
	}

	public String getMessage() {
		return message;
	}
	
	public String toString() {
		return "["+code+" : "+message+" ]";
	}
	
}