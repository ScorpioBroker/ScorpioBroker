package eu.neclab.ngsildbroker.subscriptionmanager.service;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.InternalNotification;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;

@ApplicationScoped
public class SubscriptionKafkaService extends SubscriptionKafkaServiceBase {

	@Incoming(AppConstants.ENTITY_CHANNEL)
	public void handleEntity(Message<BaseRequest> message) {
		IncomingKafkaRecordMetadata metaData = message.getMetadata(IncomingKafkaRecordMetadata.class).orElse(null);
		long timestamp = System.currentTimeMillis();
		if (metaData != null) {
			timestamp = metaData.getTimestamp().toEpochMilli();
		}
		BaseRequest payload = message.getPayload();
		handleBaseRequestEntity(payload, payload.getId(), timestamp);

	}

	@Incoming(AppConstants.INTERNAL_NOTIFICATION_CHANNEL)
	public void handleInternalNotification(Message<InternalNotification> message) {
		handleBaseRequestInternalNotification(message.getPayload());
	}
}
