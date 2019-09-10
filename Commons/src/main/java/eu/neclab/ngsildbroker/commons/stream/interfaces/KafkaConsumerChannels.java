package eu.neclab.ngsildbroker.commons.stream.interfaces;

import org.springframework.cloud.stream.annotation.Input;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.stereotype.Component;

@Component
public interface KafkaConsumerChannels {
	public String readChannel="READ_CHANNEL";
	
	@Input(readChannel)
	SubscribableChannel subscribeEntityCreate();
}
