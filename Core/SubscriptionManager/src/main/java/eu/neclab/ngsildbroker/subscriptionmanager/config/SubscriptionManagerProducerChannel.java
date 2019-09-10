package eu.neclab.ngsildbroker.subscriptionmanager.config;

import org.springframework.cloud.stream.annotation.Output;
import org.springframework.messaging.MessageChannel;

import eu.neclab.ngsildbroker.commons.stream.interfaces.IProducerChannels;

public interface SubscriptionManagerProducerChannel extends IProducerChannels {

	public String subscriptionsWriteChannel = "SUBSCRIPTIONS_WRITE_CHANNEL";
	public String atContextWriteChannel="ATCONTEXT_WRITE_CHANNEL";
	
	
//	@Output(atContextWriteChannel)
//	MessageChannel atContextWriteChannel();
	
	@Output(subscriptionsWriteChannel)
	MessageChannel subscriptionWriteChannel();

	
}
