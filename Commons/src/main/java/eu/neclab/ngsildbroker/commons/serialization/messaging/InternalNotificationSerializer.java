package eu.neclab.ngsildbroker.commons.serialization.messaging;

import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.InternalNotification;
import io.quarkus.kafka.client.serialization.ObjectMapperSerializer;

public class InternalNotificationSerializer extends ObjectMapperSerializer<InternalNotification> {

}
