package eu.neclab.ngsildbroker.subscriptionmanager.service;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import eu.neclab.ngsildbroker.commons.datatypes.InternalNotification;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import io.quarkus.arc.properties.IfBuildProperty;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;

@ApplicationScoped
@IfBuildProperty(name = "scorpio.kafka.enabled", enableIfMissing = true, stringValue = "true")
public class SubscriptionKafkaService extends SubscriptionKafkaServiceBase {

	@Incoming("entityChannel")
	public void handleEntity(Message<BaseRequest> message) {
		IncomingKafkaRecordMetadata metaData = message.getMetadata(IncomingKafkaRecordMetadata.class).orElse(null);
		if (metaData != null) {
			handleBaseRequestEntity(message.getPayload(), (String) metaData.getKey(),
					metaData.getTimestamp().toEpochMilli());
		}
	}

	@Incoming("internalNotificationChannel")
	public void handleInternalNotification(Message<InternalNotification> message) {
		IncomingKafkaRecordMetadata metaData = message.getMetadata(IncomingKafkaRecordMetadata.class).orElse(null);
		if (metaData != null) {
			handleBaseRequestInternalNotification(message.getPayload());
		}
	}
}
