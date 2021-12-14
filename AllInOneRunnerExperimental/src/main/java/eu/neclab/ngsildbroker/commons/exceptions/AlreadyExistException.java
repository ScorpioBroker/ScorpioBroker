package eu.neclab.ngsildbroker.commons.exceptions;

import eu.neclab.ngsildbroker.commons.enums.ErrorType;
/**
 * @version 1.0
 * @created 09-Jul-2018
 */
public class AlreadyExistException extends ResponseException {
	
	private static final long serialVersionUID = 1L;

	/*public AlreadyExistException(String message) {
		super(HttpStatus.CONFLICT, message);
	}*/
	
	public AlreadyExistException(ErrorType error) {
		super(error);
	}
}
