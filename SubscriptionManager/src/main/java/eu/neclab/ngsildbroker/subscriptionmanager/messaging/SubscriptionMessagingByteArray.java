package eu.neclab.ngsildbroker.subscriptionmanager.messaging;

import jakarta.inject.Singleton;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment.Strategy;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import io.quarkus.arc.profile.IfBuildProfile;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Uni;

@Singleton
@IfBuildProfile(anyOf = { "mqtt", "rabbitmq" })
public class SubscriptionMessagingByteArray extends SubscriptionMessagingBase {

	@Incoming(AppConstants.ENTITY_RETRIEVE_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	public Uni<Void> handleEntity(byte[] byteMessage) {
		return handleEntityRaw(new String(byteMessage));
	}

	@Incoming(AppConstants.INTERNAL_RETRIEVE_NOTIFICATION_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	public Uni<Void> handleInternalNotification(byte[] byteMessage) {
		return handleInternalNotificationRaw(new String(byteMessage));
	}

	@Incoming(AppConstants.ENTITY_BATCH_RETRIEVE_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	public Uni<Void> handleBatchEntities(byte[] byteMessage) {
		return handleBatchEntitiesRaw(new String(byteMessage));
	}

	@Scheduled(every = "20s", delayed = "5s")
	void purge() {
		super.purge();
	}
}
