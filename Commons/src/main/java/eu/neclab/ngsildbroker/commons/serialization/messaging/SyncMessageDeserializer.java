package eu.neclab.ngsildbroker.commons.serialization.messaging;

import eu.neclab.ngsildbroker.commons.datatypes.SyncMessage;
import io.quarkus.kafka.client.serialization.ObjectMapperDeserializer;

public class SyncMessageDeserializer extends ObjectMapperDeserializer<SyncMessage> {
	public SyncMessageDeserializer() {
		super(SyncMessage.class);
	}

}
