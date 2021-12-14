package eu.neclab.ngsildbroker.commons.exceptions;

import eu.neclab.ngsildbroker.commons.enums.ErrorType;
/**
 * @version 1.0
 * @created 09-Jul-2018
 */
public class KafkaWriteException extends ResponseException{
	
	private static final long serialVersionUID = 1L;

	/*public KafkaWriteException(String message) {
		super(HttpStatus.INTERNAL_SERVER_ERROR, message);
	}*/
	
	public KafkaWriteException(ErrorType error) {
		super(error);
	}
}
