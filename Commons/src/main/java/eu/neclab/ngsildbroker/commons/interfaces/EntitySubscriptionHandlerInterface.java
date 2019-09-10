package eu.neclab.ngsildbroker.commons.interfaces;

import java.net.URI;
import java.util.List;

import eu.neclab.ngsildbroker.commons.datatypes.CSourceRegistration;
import eu.neclab.ngsildbroker.commons.datatypes.Entity;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:40:07
 */
public interface EntitySubscriptionHandlerInterface extends SubscriptionManager, InternalNotificationHandler, CSourceNotificationHandler {

	

	/**
	 * 
	 * @param entities
	 */
	public void newData(List<Entity> entities);

	/**
	 * 
	 * @param regInfo
	 */
	public void newSource(CSourceRegistration regInfo);

	/**
	 * 
	 * @param id
	 */
	public Subscription querySubscription(URI id);

	/**
	 * 
	 * @param subscription
	 */
	public URI subscribe(Subscription subscription);

	/**
	 * 
	 * @param id
	 */
	public void unsubscribe(URI id);

	/**
	 * 
	 * @param subscription
	 */
	public Subscription updateSubscription(Subscription subscription);

}