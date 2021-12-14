package eu.neclab.ngsildbroker.commons.interfaces;

import eu.neclab.ngsildbroker.commons.datatypes.Update;
import eu.neclab.ngsildbroker.commons.datatypes.UpdateResult;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:40:01
 */
public interface UpdateHandler {

	/**
	 * 
	 * @param update
	 */
	public UpdateResult update(Update update);

}