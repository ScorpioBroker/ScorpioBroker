package eu.neclab.ngsildbroker.commons.serialization.messaging;

import eu.neclab.ngsildbroker.commons.interfaces.AnnouncementMessage;
import io.quarkus.kafka.client.serialization.ObjectMapperDeserializer;

public class AliveAnnouncementDeserializer extends ObjectMapperDeserializer<AnnouncementMessage> {
	public AliveAnnouncementDeserializer() {
		super(AnnouncementMessage.class);
	}

}
