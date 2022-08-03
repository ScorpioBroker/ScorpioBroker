package eu.neclab.ngsildbroker.commons.serialization.messaging;

import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import io.quarkus.kafka.client.serialization.ObjectMapperDeserializer;

public class SubscriptionDeserializer extends ObjectMapperDeserializer<Subscription> {
	public SubscriptionDeserializer() {
		super(Subscription.class);
	}

}
