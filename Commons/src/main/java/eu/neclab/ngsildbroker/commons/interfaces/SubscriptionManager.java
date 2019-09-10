package eu.neclab.ngsildbroker.commons.interfaces;

import java.net.URI;
import java.util.List;

import eu.neclab.ngsildbroker.commons.datatypes.Notification;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

/**
 * @author hebgen
 * @version 1.0
 * @created 11-Jun-2018 11:40:00
 */
public interface SubscriptionManager {



	/**
	 * 
	 * @param subRequest
	 * @throws AlreadyExistsException 
	 */
	public URI subscribe(SubscriptionRequest subRequest) throws ResponseException;

	/**
	 * 
	 * @param id
	 */
	public void unsubscribe(URI id) throws ResponseException;

	/**
	 * 
	 * @param subscription
	 */
	public Subscription updateSubscription(SubscriptionRequest subscription) throws ResponseException;
	
	public List<Subscription> getAllSubscriptions(int limit);
	
	public Subscription getSubscription(String subscriptionId) throws ResponseException;

	public void remoteNotify(String id, Notification notification);

}