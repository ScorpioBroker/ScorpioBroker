package eu.neclab.ngsildbroker.commons.interfaces;

import eu.neclab.ngsildbroker.commons.datatypes.Append;
import eu.neclab.ngsildbroker.commons.datatypes.AppendResult;
import eu.neclab.ngsildbroker.commons.datatypes.Create;
import eu.neclab.ngsildbroker.commons.datatypes.CreateResult;
import eu.neclab.ngsildbroker.commons.datatypes.Delete;
import eu.neclab.ngsildbroker.commons.datatypes.DeleteResult;
import eu.neclab.ngsildbroker.commons.datatypes.Query;
import eu.neclab.ngsildbroker.commons.datatypes.QueryResult;
import eu.neclab.ngsildbroker.commons.datatypes.Update;
import eu.neclab.ngsildbroker.commons.datatypes.UpdateResult;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:40:06
 */
public interface HistoryInterface extends StorageInterface {

	/**
	 * 
	 * @param append
	 */
	public AppendResult append(Append append);

	/**
	 * 
	 * @param create
	 */
	public CreateResult create(Create create);

	/**
	 * 
	 * @param delete
	 */
	public DeleteResult delete(Delete delete);

	/**
	 * 
	 * @param query
	 */
	public QueryResult query(Query query);

	/**
	 * 
	 * @param update
	 */
	public UpdateResult update(Update update);

}