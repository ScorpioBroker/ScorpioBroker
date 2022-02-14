package eu.neclab.ngsildbroker.registry.subscriptionmanager.service;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import io.quarkus.arc.properties.IfBuildProperty;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;

@ApplicationScoped
@IfBuildProperty(name = "scorpio.kafka.enabled", enableIfMissing = true, stringValue = "true")
public class RegistrySubscriptionKafkaService extends RegistrySubscriptionKafkaServiceBase {

	@Incoming(AppConstants.REGISTRY_CHANNEL)
	public void handleCsource(Message<BaseRequest> message) {
		IncomingKafkaRecordMetadata metaData = message.getMetadata(IncomingKafkaRecordMetadata.class).orElse(null);
		long timestamp = System.currentTimeMillis();
		if (metaData != null) {
			timestamp = metaData.getTimestamp().toEpochMilli();
		}
		BaseRequest payload = message.getPayload();
		handleBaseRequestRegistry(payload, payload.getId(), timestamp);
	}

	@Incoming(AppConstants.INTERNAL_SUBS_CHANNEL)
	public void handleSubscription(Message<SubscriptionRequest> message) {
		handleBaseRequestSubscription(message.getPayload(), message.getPayload().getId());
	}
}
