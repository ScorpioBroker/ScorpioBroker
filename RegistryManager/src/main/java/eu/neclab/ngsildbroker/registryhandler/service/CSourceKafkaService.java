package eu.neclab.ngsildbroker.registryhandler.service;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;

@ApplicationScoped
public class CSourceKafkaService extends CSourceKafkaServiceBase {

	@Incoming("entityChannel")
	public void handleEntity(Message<BaseRequest> message) {
		IncomingKafkaRecordMetadata metaData = message.getMetadata(IncomingKafkaRecordMetadata.class).orElse(null);
		if (metaData != null) {
			handleBaseRequest(message.getPayload(), (String) metaData.getKey());
		}
	}

}
