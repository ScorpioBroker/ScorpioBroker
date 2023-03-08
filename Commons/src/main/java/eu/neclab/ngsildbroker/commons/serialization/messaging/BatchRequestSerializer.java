package eu.neclab.ngsildbroker.commons.serialization.messaging;

import eu.neclab.ngsildbroker.commons.datatypes.requests.BatchRequest;
import io.quarkus.kafka.client.serialization.ObjectMapperSerializer;

public class BatchRequestSerializer extends ObjectMapperSerializer<BatchRequest> {

}
