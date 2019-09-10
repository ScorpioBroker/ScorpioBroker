package eu.neclab.ngsildbroker.historymanager.config;

import org.springframework.cloud.stream.annotation.Input;
import org.springframework.messaging.SubscribableChannel;

import eu.neclab.ngsildbroker.commons.stream.interfaces.IConsumerChannels;

/**
 * 
 * @version 1.0
 * @date 10-Jul-2018
 */
public interface ConsumerChannel extends IConsumerChannels {

	public String createReadChannel = "CREATE_READ_CHANNEL";
	public String appendReadChannel = "APPEND_READ_CHANNEL";
	public String updateReadChannel = "UPDATE_READ_CHANNEL";
	public String deleteReadChannel = "DELETE_READ_CHANNEL";

	@Input(createReadChannel)
	SubscribableChannel createReadChannel();

	@Input(appendReadChannel)
	SubscribableChannel appendReadChannel();

	@Input(updateReadChannel)
	SubscribableChannel updateReadChannel();

	@Input(deleteReadChannel)
	SubscribableChannel deleteReadChannel();
	

}
