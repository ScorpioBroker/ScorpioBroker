package eu.neclab.ngsildbroker.registryhandler.config;

import org.springframework.cloud.stream.annotation.Input;
import org.springframework.messaging.SubscribableChannel;

import eu.neclab.ngsildbroker.commons.stream.interfaces.IConsumerChannels;

public interface CSourceConsumerChannel extends IConsumerChannels {
	public String csourceReadChannel = "CSOURCE_REGISTRATION_READ_CHANNEL";
	public String contextRegistryReadChannel = "CONTEXT_REGISTRY_READ_CHANNEL";
	public String contextUpdateReadChannel = "CONTEXT_UPDATE_READ_CHANNEL";

	@Input(csourceReadChannel)
	SubscribableChannel csourceReadChannel();

	@Input(contextRegistryReadChannel)
	SubscribableChannel contextRegistryReadChannel();

	@Input(contextUpdateReadChannel)
	SubscribableChannel contextUpdateReadChannel();
}
