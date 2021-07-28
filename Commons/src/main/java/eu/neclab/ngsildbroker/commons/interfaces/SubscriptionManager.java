package eu.neclab.ngsildbroker.commons.interfaces;

import java.net.URI;
import java.util.List;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.datatypes.Notification;
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
	 * @param headers 
	 */
	public void unsubscribe(URI id, ArrayListMultimap<String,String> headers) throws ResponseException;

	/**
	 * 
	 * @param subscription
	 */
	public SubscriptionRequest updateSubscription(SubscriptionRequest subscription) throws ResponseException;
	
	public List<SubscriptionRequest> getAllSubscriptions(int limit, ArrayListMultimap<String, String> headers);
	
	public SubscriptionRequest getSubscription(String subscriptionId, ArrayListMultimap<String, String> headers) throws ResponseException;

	public void remoteNotify(String id, Notification notification);

}