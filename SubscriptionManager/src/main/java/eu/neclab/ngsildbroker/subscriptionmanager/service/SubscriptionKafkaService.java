package eu.neclab.ngsildbroker.subscriptionmanager.service;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.InternalNotification;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;

@ApplicationScoped
public class SubscriptionKafkaService extends SubscriptionKafkaServiceBase {

	@Incoming(AppConstants.ENTITY_RETRIEVE_CHANNEL)
	public Uni<Void> handleEntity(Message<BaseRequest> message) {
		IncomingKafkaRecordMetadata metaData = message.getMetadata(IncomingKafkaRecordMetadata.class).orElse(null);
		long timestamp = System.currentTimeMillis();
		if (metaData != null) {
			timestamp = metaData.getTimestamp().toEpochMilli();
		}
		BaseRequest payload = message.getPayload();
		handleBaseRequestEntity(payload, payload.getId(), timestamp);
		return Uni.createFrom().nullItem();
	}

	@Incoming(AppConstants.INTERNAL_RETRIEVE_NOTIFICATION_CHANNEL)
	public Uni<Void> handleInternalNotification(Message<InternalNotification> message) {
		handleBaseRequestInternalNotification(message.getPayload());
		return Uni.createFrom().nullItem();
	}
}
