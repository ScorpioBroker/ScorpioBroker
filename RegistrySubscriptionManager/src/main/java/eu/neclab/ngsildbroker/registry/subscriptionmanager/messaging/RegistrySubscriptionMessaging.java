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
@IfBuildProfile(anyOf = { "sqs", "mqtt", "rabbitmq" })
public class RegistrySubscriptionMessaging extends RegistrySubscriptionMessagingBase {

	@Incoming(AppConstants.REGISTRY_RETRIEVE_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	public Uni<Void> handleCsource(Object byteMessage) {
		if (byteMessage instanceof byte[] bytes) {
			byteMessage = new String(bytes);
		}
		return handleCsourceRaw((String) byteMessage);
	}

	@Incoming(AppConstants.INTERNAL_RETRIEVE_SUBS_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	public Uni<Void> handleSubscription(Object byteMessage) {
		if (byteMessage instanceof byte[] bytes) {
			byteMessage = new String(bytes);
		}
		return handleSubscriptionRaw((String) byteMessage);
	}

	@Scheduled(every = "20s", delayed = "5s")
	void purge() {
		super.purge();
	}
}
