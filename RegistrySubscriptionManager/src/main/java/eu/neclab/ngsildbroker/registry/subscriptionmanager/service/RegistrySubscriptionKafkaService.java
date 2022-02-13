package eu.neclab.ngsildbroker.registry.subscriptionmanager.service;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import io.quarkus.arc.properties.IfBuildProperty;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;

@ApplicationScoped
@IfBuildProperty(name = "scorpio.kafka.enabled", enableIfMissing = true, stringValue = "true")
public class RegistrySubscriptionKafkaService extends RegistrySubscriptionKafkaServiceBase {

	@Incoming("registryChannel")
	public void handleCsource(Message<BaseRequest> message) {
		IncomingKafkaRecordMetadata metaData = message.getMetadata(IncomingKafkaRecordMetadata.class).orElse(null);
		handleBaseRequestRegistry(message.getPayload(), (String) metaData.getKey(),
				metaData.getTimestamp().toEpochMilli());
	}

	@Incoming("internalRegistrySubscriptionChannel")
	public void handleSubscription(Message<SubscriptionRequest> message) {
		IncomingKafkaRecordMetadata metaData = message.getMetadata(IncomingKafkaRecordMetadata.class).orElse(null);
		handleBaseRequestSubscription(message.getPayload(), (String) metaData.getKey());
	}
}
