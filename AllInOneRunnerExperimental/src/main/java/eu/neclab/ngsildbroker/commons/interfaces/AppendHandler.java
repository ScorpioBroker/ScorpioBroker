package eu.neclab.ngsildbroker.commons.interfaces;

import eu.neclab.ngsildbroker.commons.datatypes.Append;
import eu.neclab.ngsildbroker.commons.datatypes.AppendResult;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:39:57
 */
public interface AppendHandler {

	/**
	 * 
	 * @param append
	 */
	public AppendResult append(Append append);

}