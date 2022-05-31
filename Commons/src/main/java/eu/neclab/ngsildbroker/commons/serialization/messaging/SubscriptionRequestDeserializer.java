package eu.neclab.ngsildbroker.commons.serialization.messaging;

import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import io.quarkus.kafka.client.serialization.ObjectMapperDeserializer;

public class SubscriptionRequestDeserializer extends ObjectMapperDeserializer<SubscriptionRequest> {
	public SubscriptionRequestDeserializer() {
		super(SubscriptionRequest.class);
	}

}
