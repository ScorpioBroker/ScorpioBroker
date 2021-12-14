package eu.neclab.ngsildbroker.commons.exceptions;

import eu.neclab.ngsildbroker.commons.enums.ErrorType;
/**
 * @version 1.0
 * @date 9-Jul-2018
 */
public class NotFoundException extends ResponseException{
	private static final long serialVersionUID = 1L;

	public NotFoundException(ErrorType error) {
		super(error);
		
	}
	/*public NotFoundException(String message) {
		super(HttpStatus.NOT_FOUND, message);
	}*/

}
