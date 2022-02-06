package eu.neclab.ngsildbroker.subscriptionmanager.service;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import eu.neclab.ngsildbroker.commons.datatypes.InternalNotification;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;

@Service
@ConditionalOnProperty(prefix = "scorpio.kafka", matchIfMissing = true, name = "enabled", havingValue = "true")
public class SubscriptionKafkaService extends SubscriptionKafkaServiceBase {

	@KafkaListener(topics = "${scorpio.topics.entity}", groupId = "subscription")
	public void handleEntity(@Payload BaseRequest message, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
			@Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timeStamp) {
		handleBaseRequestEntity(message, key, timeStamp);
	}

	@KafkaListener(topics = "${scorpio.topics.internalnotification}", groupId = "subscription")
	public void handleInternalNotification(@Payload InternalNotification message,
			@Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
			@Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timeStamp) {
		handleBaseRequestInternalNotification(message);
	}
}
