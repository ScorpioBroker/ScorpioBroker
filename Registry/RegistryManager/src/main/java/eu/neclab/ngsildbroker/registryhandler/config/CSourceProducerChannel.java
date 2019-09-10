package eu.neclab.ngsildbroker.registryhandler.config;

import org.springframework.cloud.stream.annotation.Output;
import org.springframework.messaging.MessageChannel;

import eu.neclab.ngsildbroker.commons.stream.interfaces.IProducerChannels;

public interface CSourceProducerChannel extends IProducerChannels {
	public String csourceWriteChannel = "CSOURCE_REGISTRATION_WRITE_CHANNEL";
	public String csourceSubscriptionWriteChannel = "CSOURCE_SUBSCRIPTION_WRITE_CHANNEL";
	public String csourceNotificationWriteChannel = "CSOURCE_NOTIFICATION_WRITE_CHANNEL";

	@Output(csourceWriteChannel)
	MessageChannel csourceWriteChannel();
	
	@Output(csourceSubscriptionWriteChannel)
	MessageChannel csourceSubscriptionWriteChannel();
	
	@Output(csourceNotificationWriteChannel)
	MessageChannel csourceNotificationWriteChannel();
	
}
