package eu.neclab.ngsildbroker.registry.subscriptionmanager.messaging;

import javax.inject.Singleton;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import io.quarkus.arc.profile.IfBuildProfile;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;

@Singleton
@IfBuildProfile("kafka")
public class RegistrySubscriptionMessagingKafka extends RegistrySubscriptionMessagingBase {

	@Incoming(AppConstants.REGISTRY_RETRIEVE_CHANNEL)
	@IfBuildProfile("kafka")
	public Uni<Void> handleCsource(Message<BaseRequest> busMessage) {
		@SuppressWarnings("unchecked")
		IncomingKafkaRecordMetadata<String, Object> metaData = busMessage.getMetadata(IncomingKafkaRecordMetadata.class)
				.orElse(null);
		
		final long timestamp;
		if (metaData != null) {
			timestamp = metaData.getTimestamp().toEpochMilli();
		} else {
			timestamp = System.currentTimeMillis();
		}
		return baseHandleCsource(busMessage, timestamp);
	}

	@Incoming(AppConstants.INTERNAL_RETRIEVE_SUBS_CHANNEL)
	@IfBuildProfile("kafka")
	public Uni<Void> handleSubscription(Message<SubscriptionRequest> busMessage) {
		return baseHandleSubscription(busMessage);
	}
}
