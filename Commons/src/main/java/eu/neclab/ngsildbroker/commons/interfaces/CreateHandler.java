package eu.neclab.ngsildbroker.commons.interfaces;

import eu.neclab.ngsildbroker.commons.datatypes.Create;
import eu.neclab.ngsildbroker.commons.datatypes.CreateResult;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:39:59
 */
public interface CreateHandler {

	/**
	 * 
	 * @param create

	 */
	public CreateResult create(Create create);

}