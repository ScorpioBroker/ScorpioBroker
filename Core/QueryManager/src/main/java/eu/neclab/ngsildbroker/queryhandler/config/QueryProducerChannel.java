package eu.neclab.ngsildbroker.queryhandler.config;

import org.springframework.cloud.stream.annotation.Output;
import org.springframework.messaging.MessageChannel;

import eu.neclab.ngsildbroker.commons.stream.interfaces.IProducerChannels;

/**
 * 
 * @version 1.0
 * @date 10-Jul-2018
 */
public interface QueryProducerChannel extends IProducerChannels {

	public String paginationWriteChannel = "PAGINATION";
	
	

	@Output(paginationWriteChannel)
	MessageChannel paginationWriteChannel();

		
	
}
