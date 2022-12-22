package eu.neclab.ngsildbroker.commons.interfaces;

import java.util.List;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import io.smallrye.mutiny.Uni;
import io.vertx.core.MultiMap;

public interface SubscriptionCRUDService {

	Uni<String> subscribe(SubscriptionRequest subRequest);

	Uni<List<SubscriptionRequest>> getAllSubscriptions(MultiMap headers);

	Uni<SubscriptionRequest> getSubscription(String id, MultiMap headers);

	Uni<Void> unsubscribe(String id, MultiMap headers);

	Uni<Void> updateSubscription(SubscriptionRequest subscriptionRequest);

}
