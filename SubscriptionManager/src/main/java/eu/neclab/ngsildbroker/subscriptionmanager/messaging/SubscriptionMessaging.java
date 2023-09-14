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
@IfBuildProfile(anyOf = { "sqs", "mqtt", "rabbitmq" })
public class SubscriptionMessaging extends SubscriptionMessagingBase {

	@Incoming(AppConstants.ENTITY_RETRIEVE_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	public Uni<Void> handleEntity(Object byteMessage) {
		if (byteMessage instanceof byte[] bytes) {
			byteMessage = new String(bytes);
		}
		return handleEntityRaw((String) byteMessage);
	}

	@Incoming(AppConstants.INTERNAL_RETRIEVE_NOTIFICATION_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	public Uni<Void> handleInternalNotification(Object byteMessage) {
		if (byteMessage instanceof byte[] bytes) {
			byteMessage = new String(bytes);
		}
		return handleInternalNotificationRaw((String) byteMessage);
	}

	@Incoming(AppConstants.ENTITY_BATCH_RETRIEVE_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	public Uni<Void> handleBatchEntities(Object byteMessage) {
		if (byteMessage instanceof byte[] bytes) {
			byteMessage = new String(bytes);
		}
		return handleBatchEntitiesRaw((String) byteMessage);
	}

	@Scheduled(every = "20s", delayed = "5s")
	void purge() {
		super.purge();
	}
}
