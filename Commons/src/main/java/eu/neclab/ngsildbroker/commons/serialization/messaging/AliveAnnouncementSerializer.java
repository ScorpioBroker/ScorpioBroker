package eu.neclab.ngsildbroker.commons.serialization.messaging;

import eu.neclab.ngsildbroker.commons.interfaces.AnnouncementMessage;
import io.quarkus.kafka.client.serialization.ObjectMapperSerializer;

public class AliveAnnouncementSerializer extends ObjectMapperSerializer<AnnouncementMessage> {

}
