package eu.neclab.ngsildbroker.commons.serialization.messaging;

import eu.neclab.ngsildbroker.commons.datatypes.requests.BatchRequest;
import io.quarkus.kafka.client.serialization.ObjectMapperDeserializer;

public class BatchRequestDeserializer extends ObjectMapperDeserializer<BatchRequest> {
	public BatchRequestDeserializer() {
		super(BatchRequest.class);
	}

}
