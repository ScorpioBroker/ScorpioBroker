package eu.neclab.ngsildbroker.commons.serialization.messaging;

import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.SubscriptionRequest;
import io.quarkus.kafka.client.serialization.ObjectMapperSerializer;

public class SubscriptionRequestSerializer extends ObjectMapperSerializer<SubscriptionRequest> {

}
