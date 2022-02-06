package eu.neclab.ngsildbroker.registry.subscriptionmanager.service;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;

@Service
@ConditionalOnProperty(prefix = "scorpio.kafka", matchIfMissing = true, name = "enabled", havingValue = "true")
public class RegistrySubscriptionKafkaService extends RegistrySubscriptionKafkaServiceBase {

	@KafkaListener(topics = "${scorpio.topics.registry}", groupId = "csourcesubscription")
	public void handleCsource(@Payload BaseRequest message, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
			@Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timeStamp) {
		handleBaseRequestRegistry(message, key, timeStamp);
	}

	@KafkaListener(topics = "${scorpio.topics.internalregsub}", groupId = "csourcesubscription")
	public void handleSubscription(@Payload SubscriptionRequest message,
			@Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
			@Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timeStamp) {
		handleBaseRequestSubscription(message, key);
	}
}
