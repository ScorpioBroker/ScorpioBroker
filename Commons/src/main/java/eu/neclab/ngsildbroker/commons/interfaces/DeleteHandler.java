package eu.neclab.ngsildbroker.commons.interfaces;

import eu.neclab.ngsildbroker.commons.datatypes.Delete;
import eu.neclab.ngsildbroker.commons.datatypes.DeleteResult;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:40:01
 */
public interface DeleteHandler {

	/**
	 * 
	 * @param delete
	 */
	public DeleteResult delete(Delete delete);

}