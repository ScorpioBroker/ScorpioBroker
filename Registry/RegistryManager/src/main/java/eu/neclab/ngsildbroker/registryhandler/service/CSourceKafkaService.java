package eu.neclab.ngsildbroker.registryhandler.service;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;

@Service
@ConditionalOnProperty(prefix = "scorpio.registry", name = "autorecording", matchIfMissing = true, havingValue = "active")
public class CSourceKafkaService extends CSourceKafkaServiceBase {

	@KafkaListener(topics = "${scorpio.topics.entity}", groupId = "csource")
	public void handleEntity(@Payload BaseRequest message, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
			@Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timeStamp) {
		handleBaseRequest(message, key);
	}

}
