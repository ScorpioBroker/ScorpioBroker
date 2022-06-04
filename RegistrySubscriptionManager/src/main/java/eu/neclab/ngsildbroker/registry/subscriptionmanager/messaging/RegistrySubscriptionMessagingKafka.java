package eu.neclab.ngsildbroker.registry.subscriptionmanager.messaging;

import javax.inject.Singleton;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import io.quarkus.arc.profile.IfBuildProfile;
import io.smallrye.mutiny.Uni;

@Singleton
@IfBuildProfile("kafka")
public class RegistrySubscriptionMessagingKafka extends RegistrySubscriptionMessagingBase {

	@Incoming(AppConstants.REGISTRY_RETRIEVE_CHANNEL)
	@IfBuildProfile("kafka")
	public Uni<Void> handleCsource(Message<BaseRequest> busMessage) {
		return baseHandleCsource(busMessage);
	}

	@Incoming(AppConstants.INTERNAL_RETRIEVE_SUBS_CHANNEL)
	@IfBuildProfile("kafka")
	public Uni<Void> handleSubscription(Message<SubscriptionRequest> busMessage) {
		return baseHandleSubscription(busMessage);
	}
}
