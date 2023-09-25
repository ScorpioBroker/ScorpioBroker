package eu.neclab.ngsildbroker.registry.subscriptionmanager.messaging;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment.Strategy;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import io.quarkus.arc.profile.IfBuildProfile;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Singleton;

@Singleton
@IfBuildProfile(anyOf = { "mqtt", "rabbitmq" })
public class RegistrySubscriptionMessagingByteArray extends RegistrySubscriptionMessagingBase {

	@Incoming(AppConstants.REGISTRY_RETRIEVE_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	public Uni<Void> handleCsource(byte[] byteMessage) {
		return handleCsourceRaw(new String(byteMessage));
	}

	@Incoming(AppConstants.INTERNAL_RETRIEVE_SUBS_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	public Uni<Void> handleSubscription(byte[] byteMessage) {
		return handleSubscriptionRaw(new String(byteMessage));
	}

	@Scheduled(every = "20s", delayed = "5s")
	void purge() {
		super.purge();
	}
}
