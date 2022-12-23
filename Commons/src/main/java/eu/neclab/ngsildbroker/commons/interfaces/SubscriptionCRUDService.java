package eu.neclab.ngsildbroker.commons.interfaces;

import java.util.List;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import io.smallrye.mutiny.Uni;

public interface SubscriptionCRUDService {

	Uni<String> subscribe(SubscriptionRequest subRequest);

	Uni<List<SubscriptionRequest>> getAllSubscriptions(String tenant);

	Uni<SubscriptionRequest> getSubscription(String id, String tenant);

	Uni<Void> unsubscribe(String id, String tenant);

	Uni<Void> updateSubscription(SubscriptionRequest subscriptionRequest);

}
