package eu.neclab.ngsildbroker.commons.interfaces;

import java.util.List;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;

public interface SubscriptionCRUDService {

	String subscribe(SubscriptionRequest subRequest) throws ResponseException;

	List<SubscriptionRequest> getAllSubscriptions(ArrayListMultimap<String, String> headers);

	SubscriptionRequest getSubscription(String id, ArrayListMultimap<String, String> headers) throws ResponseException;

	void unsubscribe(String id, ArrayListMultimap<String, String> headers) throws ResponseException;

	void updateSubscription(SubscriptionRequest subscriptionRequest) throws ResponseException;

}
