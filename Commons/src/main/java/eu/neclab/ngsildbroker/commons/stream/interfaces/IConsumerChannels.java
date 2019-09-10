package eu.neclab.ngsildbroker.commons.stream.interfaces;

import org.springframework.cloud.stream.annotation.Input;
import org.springframework.messaging.SubscribableChannel;

public interface IConsumerChannels {
	public String readChannel = "";

	@Input(readChannel)
	SubscribableChannel subscribeEntityCreate();
}
