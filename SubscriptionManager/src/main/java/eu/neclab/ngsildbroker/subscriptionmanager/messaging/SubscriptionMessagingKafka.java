package eu.neclab.ngsildbroker.subscriptionmanager.messaging;

import javax.inject.Singleton;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.InternalNotification;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import io.quarkus.arc.profile.UnlessBuildProfile;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;

@Singleton
@UnlessBuildProfile("in-memory")
public class SubscriptionMessagingKafka extends SubscriptionMessagingBase {

	@Incoming(AppConstants.ENTITY_RETRIEVE_CHANNEL)
	public Uni<Void> handleEntity(Message<BaseRequest> message) {
		@SuppressWarnings("unchecked")
		IncomingKafkaRecordMetadata<String, Object> metaData = message.getMetadata(IncomingKafkaRecordMetadata.class)
				.orElse(null);
		final long timestamp;
		if (metaData != null) {
			timestamp = metaData.getTimestamp().toEpochMilli();
		} else {
			timestamp = System.currentTimeMillis();
		}
		return baseHandleEntity(message, timestamp);
	}

	@Incoming(AppConstants.INTERNAL_RETRIEVE_NOTIFICATION_CHANNEL)
	public Uni<Void> handleInternalNotification(Message<InternalNotification> message) {
		return baseHandleInternalNotification(message);
	}
}
