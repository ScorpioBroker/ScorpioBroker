package eu.neclab.ngsildbroker.commons.interfaces;

import eu.neclab.ngsildbroker.commons.datatypes.Notification;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:40:09
 */
public interface NotificationHandler {

	/**
	 * 
	 * @param notification
	 * @param ldContext 
	 */

	void notify(Notification notification, SubscriptionRequest subscriptionRequest);

}