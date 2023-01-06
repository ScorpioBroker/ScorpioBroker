package eu.neclab.ngsildbroker.commons.interfaces;

import java.util.List;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.SubscriptionRequest;
import io.smallrye.mutiny.Uni;

public interface SubscriptionCRUDService {

	Uni<String> subscribe(SubscriptionRequest subRequest);

	Uni<List<SubscriptionRequest>> getAllSubscriptions(ArrayListMultimap<String, String> headers);

	Uni<SubscriptionRequest> getSubscription(String id, ArrayListMultimap<String, String> headers);

	Uni<Void> unsubscribe(String id, ArrayListMultimap<String, String> headers);

	Uni<Void> updateSubscription(SubscriptionRequest subscriptionRequest);

}
