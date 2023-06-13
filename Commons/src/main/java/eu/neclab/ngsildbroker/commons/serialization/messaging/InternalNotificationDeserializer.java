package eu.neclab.ngsildbroker.commons.serialization.messaging;

import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.InternalNotification;
import io.quarkus.kafka.client.serialization.ObjectMapperDeserializer;

public class InternalNotificationDeserializer extends ObjectMapperDeserializer<InternalNotification> {
	public InternalNotificationDeserializer() {
		super(InternalNotification.class);
	}

}
