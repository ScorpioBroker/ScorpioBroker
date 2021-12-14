package eu.neclab.ngsildbroker.commons.exceptions;

import org.springframework.http.HttpStatus;

import eu.neclab.ngsildbroker.commons.enums.ErrorType;

/**
 * @version 1.0
 * @created 09-Jul-2018
 */
public class ResponseException extends Exception{
	private static final long serialVersionUID = 1L;
	
	private HttpStatus httpStatus; 
	private ErrorType error;
	
	public ResponseException(String message) {
        super(message);
    }
	
	/*public ResponseException(int code,String message) {
        super(message);
        this.httpStatus=HttpStatus.valueOf(code);
    }*/
	
	public ResponseException(ErrorType error) {
		super(error.getMessage());
		this.error=error;
		this.httpStatus=HttpStatus.valueOf(error.getCode());
	}
	
	public ResponseException(ErrorType error,String errorMessage) {
		super(errorMessage);
		this.error=error;
		this.httpStatus=HttpStatus.valueOf(error.getCode());
	}
	
	/*public ResponseException(HttpStatus httpStatus,String message) {
        super(message);
        this.httpStatus=httpStatus;
    }*/

	public HttpStatus getHttpStatus() {
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
