package eu.neclab.ngsildbroker.commons.interfaces;

import eu.neclab.ngsildbroker.commons.datatypes.CSourceQueryResult;
import eu.neclab.ngsildbroker.commons.datatypes.Query;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:40:02
 */
public interface CSourceQueryHandler {

	/**
	 * 
	 * @param query
	 */
	public CSourceQueryResult query(Query query);

}