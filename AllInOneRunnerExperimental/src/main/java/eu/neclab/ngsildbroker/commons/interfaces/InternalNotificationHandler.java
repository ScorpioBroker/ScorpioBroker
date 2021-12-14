package eu.neclab.ngsildbroker.commons.interfaces;

import java.util.List;

import eu.neclab.ngsildbroker.commons.datatypes.Entity;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:40:08
 */
public interface InternalNotificationHandler {

	/**
	 * 
	 * @param entities
	 */
	public void newData(List<Entity> entities);

}