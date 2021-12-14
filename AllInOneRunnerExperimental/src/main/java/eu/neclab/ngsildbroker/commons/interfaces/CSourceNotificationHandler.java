package eu.neclab.ngsildbroker.commons.interfaces;

import eu.neclab.ngsildbroker.commons.datatypes.CSourceNotification;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:40:08
 */
public interface CSourceNotificationHandler {

	/**
	 * 
	 * @param regInfo
	 */
	public void notify(CSourceNotification notification, Subscription sub);

}