package eu.neclab.ngsildbroker.registry.subscriptionmanager.messaging;

import javax.inject.Singleton;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import io.quarkus.arc.profile.IfBuildProfile;
import io.quarkus.arc.profile.UnlessBuildProfile;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;

@Singleton
@UnlessBuildProfile("in-memory")
public class RegistrySubscriptionMessagingKafka extends RegistrySubscriptionMessagingBase {

	@ConfigProperty(name = "scorpio.messaging.duplicate", defaultValue = "false")
	boolean duplicate;

	@Incoming(AppConstants.REGISTRY_RETRIEVE_CHANNEL)
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
		if (duplicate) {
			return baseHandleCsource(MicroServiceUtils.deepCopyRequestMessage(busMessage), timestamp);
		}

		return baseHandleCsource(busMessage, timestamp);
	}

	@Incoming(AppConstants.INTERNAL_RETRIEVE_SUBS_CHANNEL)
	public Uni<Void> handleSubscription(Message<SubscriptionRequest> busMessage) {
		if (duplicate) {
			return baseHandleSubscription(MicroServiceUtils.deepCopySubscriptionMessage(busMessage));
		}
		return baseHandleSubscription(busMessage);
	}
}
