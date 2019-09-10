package eu.neclab.ngsildbroker.subscriptionmanager.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.messaging.SubscribableChannel;

import eu.neclab.ngsildbroker.commons.stream.interfaces.IConsumerChannels;

public interface SubscriptionManagerConsumerChannel extends IConsumerChannels {

	
	@Value("${bootstrap.servers}")
	public String entityReadChannel = "ENTITY_READ_CHANNEL";
	public String createReadChannel = "CREATE_READ_CHANNEL";
	public String appendReadChannel = "APPEND_READ_CHANNEL";
	public String updateReadChannel = "UPDATE_READ_CHANNEL";
	public String deleteReadChannel = "DELETE_READ_CHANNEL";
	public String subscriptionsReadChannel = "SUBSCRIPTIONS_READ_CHANNEL";

	@Input(entityReadChannel)
	SubscribableChannel entityReadChannel();
	
	@Input(createReadChannel)
	SubscribableChannel createReadChannel();
	
	@Input(appendReadChannel)
	SubscribableChannel appendReadChannel();
	
	@Input(updateReadChannel)
	SubscribableChannel updateReadChannel();
	
	@Input(subscriptionsReadChannel)
	SubscribableChannel subscriptionsReadChannel();

}
