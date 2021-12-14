package eu.neclab.ngsildbroker.commons.interfaces;

import eu.neclab.ngsildbroker.commons.datatypes.Query;
import eu.neclab.ngsildbroker.commons.datatypes.QueryResult;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:40:02
 */
public interface QueryHandler {

	/**
	 * 
	 * @param query
	 */
	public QueryResult query(Query query);

}