package eu.neclab.ngsildbroker.commons.serialization.messaging;

import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import io.quarkus.kafka.client.serialization.ObjectMapperDeserializer;

public class BaseRequestDeserializer extends ObjectMapperDeserializer<BaseRequest> {
	public BaseRequestDeserializer() {
		super(BaseRequest.class);
	}

}
