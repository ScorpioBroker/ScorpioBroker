package eu.neclab.ngsildbroker.commons.serialization.messaging;

import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import io.quarkus.kafka.client.serialization.ObjectMapperSerializer;

public class BaseRequestSerializer extends ObjectMapperSerializer<BaseRequest> {

}
