package eu.neclab.ngsildbroker.queryhandler.config;

import org.springframework.cloud.stream.annotation.Input;
import org.springframework.messaging.SubscribableChannel;
import eu.neclab.ngsildbroker.commons.stream.interfaces.IConsumerChannels;

public interface QueryConsumerChannel extends IConsumerChannels {

	public String entityChannel = "ENTITY_CHANNEL";

	@Input(entityChannel)
	SubscribableChannel entityChannel();

}
