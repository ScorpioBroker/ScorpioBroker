package eu.neclab.ngsildbroker.historymanager.config;

import org.springframework.cloud.stream.annotation.Output;
import org.springframework.messaging.MessageChannel;

import eu.neclab.ngsildbroker.commons.stream.interfaces.IProducerChannels;

public interface ProducerChannel extends IProducerChannels {

	public String temporalEntityWriteChannel = "TEMPORAL_ENTITY_WRITE_CHANNEL";

	@Output(temporalEntityWriteChannel)
	MessageChannel temporalEntityWriteChannel();

}
