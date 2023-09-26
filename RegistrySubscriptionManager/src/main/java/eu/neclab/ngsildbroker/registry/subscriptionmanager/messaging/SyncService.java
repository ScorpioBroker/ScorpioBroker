package eu.neclab.ngsildbroker.registry.subscriptionmanager.messaging;

import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.DeleteSubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.SubscriptionRequest;
import io.smallrye.mutiny.Uni;

public interface SyncService {

	Uni<Void> sync(SubscriptionRequest request);

}
