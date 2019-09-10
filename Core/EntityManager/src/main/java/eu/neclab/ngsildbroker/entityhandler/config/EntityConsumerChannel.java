package eu.neclab.ngsildbroker.entityhandler.config;

import org.springframework.cloud.stream.annotation.Input;
import org.springframework.messaging.SubscribableChannel;

import eu.neclab.ngsildbroker.commons.stream.interfaces.IConsumerChannels;

/**
 * 
 * @version 1.0
 * @date 10-Jul-2018
 */
public interface EntityConsumerChannel extends IConsumerChannels {

	public String entityReadChannel = "ENTITY_READ_CHANNEL";
	public String createReadChannel = "CREATE_READ_CHANNEL";
	public String appendReadChannel = "APPEND_READ_CHANNEL";
	public String updateReadChannel = "UPDATE_READ_CHANNEL";
	public String deleteReadChannel = "DELETE_READ_CHANNEL";
	public String entityIndexReadChannel="INDEX_READ_CHANNEL";
			

	@Input(entityReadChannel)
	SubscribableChannel entityReadChannel();

	@Input(createReadChannel)
	SubscribableChannel createReadChannel();

	@Input(appendReadChannel)
	SubscribableChannel appendReadChannel();

	@Input(updateReadChannel)
	SubscribableChannel updateReadChannel();

	@Input(deleteReadChannel)
	SubscribableChannel deleteReadChannel();
	
	@Input(entityIndexReadChannel)
	SubscribableChannel entityIndexReadChannel();

}
