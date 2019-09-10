package eu.neclab.ngsildbroker.commons.exceptions;

import eu.neclab.ngsildbroker.commons.enums.ErrorType;
/**
 * @version 1.0
 * @created 09-Jul-2018
 */
public class BadRequestException extends ResponseException{

	private static final long serialVersionUID = 1L;

	
	
	public BadRequestException() {
		super(ErrorType.BadRequestData);
	}

}
