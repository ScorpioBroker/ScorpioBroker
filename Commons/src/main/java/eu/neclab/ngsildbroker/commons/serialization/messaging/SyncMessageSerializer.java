package eu.neclab.ngsildbroker.commons.serialization.messaging;

import eu.neclab.ngsildbroker.commons.datatypes.SyncMessage;
import io.quarkus.kafka.client.serialization.ObjectMapperSerializer;

public class SyncMessageSerializer extends ObjectMapperSerializer<SyncMessage> {

}
