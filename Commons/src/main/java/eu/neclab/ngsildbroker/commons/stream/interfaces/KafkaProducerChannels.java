package eu.neclab.ngsildbroker.commons.stream.interfaces;

import org.springframework.cloud.stream.annotation.Output;
import org.springframework.messaging.MessageChannel;
import org.springframework.stereotype.Component;

@Component
public interface KafkaProducerChannels extends IProducerChannels{
	
	String writeChannel = "CREATE_WRITE_CHANNEL";
	
	@Output(writeChannel)
	MessageChannel entityCreate();
}
