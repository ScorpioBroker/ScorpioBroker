package eu.neclab.ngsildbroker.subscriptionmanager.messaging;

import jakarta.inject.Singleton;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment.Strategy;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import io.quarkus.arc.profile.IfBuildProfile;
import io.smallrye.mutiny.Uni;

@Singleton
@IfBuildProfile(anyOf = { "kafka", "sqs" })
public class SubscriptionMessagingStringMsg extends SubscriptionMessagingBase {

	@Incoming(AppConstants.ENTITY_RETRIEVE_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	public Uni<Void> handleEntity(String byteMessage) {
		return handleEntityRaw(byteMessage);
	}

	@Incoming(AppConstants.INTERNAL_RETRIEVE_NOTIFICATION_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	public Uni<Void> handleInternalNotification(String byteMessage) {
		return handleInternalNotificationRaw(byteMessage);
	}

	@Incoming(AppConstants.ENTITY_BATCH_RETRIEVE_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	public Uni<Void> handleBatchEntities(String byteMessage) {
		return handleBatchEntitiesRaw(byteMessage);
	}

}
