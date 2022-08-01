package eu.neclab.ngsildbroker.commons.serialization.messaging;

import eu.neclab.ngsildbroker.commons.datatypes.AliveAnnouncement;
import io.quarkus.kafka.client.serialization.ObjectMapperDeserializer;

public class AliveAnnouncementDeserializer extends ObjectMapperDeserializer<AliveAnnouncement> {
	public AliveAnnouncementDeserializer() {
		super(AliveAnnouncement.class);
	}

}
