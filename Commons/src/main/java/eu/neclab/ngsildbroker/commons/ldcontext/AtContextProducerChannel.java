package eu.neclab.ngsildbroker.commons.ldcontext;

import org.springframework.cloud.stream.annotation.Output;
import org.springframework.messaging.MessageChannel;



public interface AtContextProducerChannel {
	
	
	public String atContextWriteChannel="ATCONTEXT";
	
	
	@Output(atContextWriteChannel)
	MessageChannel atContextWriteChannel();

}
