package eu.neclab.ngsildbroker.entityhandler.config;

import org.springframework.cloud.stream.annotation.Output;
import org.springframework.messaging.MessageChannel;

import eu.neclab.ngsildbroker.commons.stream.interfaces.IProducerChannels;

/**
 * 
 * @version 1.0
 * @date 10-Jul-2018
 */

public interface EntityProducerChannel extends IProducerChannels {

	public String entityWriteChannel = "ENTITY_WRITE_CHANNEL";
	public String kvEntityWriteChannel = "KVENTITY_WRITE_CHANNEL";
	public String entityWithoutSysAttrsWriteChannel = "ENTITY_WITHOUT_SYSATTRS_WRITE_CHANNEL";
	public String createWriteChannel = "CREATE_WRITE_CHANNEL";
	public String appendWriteChannel = "APPEND_WRITE_CHANNEL";
	public String updateWriteChannel = "UPDATE_WRITE_CHANNEL";
	public String deleteWriteChannel = "DELETE_WRITE_CHANNEL";
	public String contextRegistryWriteChannel="CONTEXT_REGISTRY_WRITE_CHANNEL";
	public String contextUpdateWriteChannel="CONTEXT_REGISTRY_UPDATE_CHANNEL";
	public String entityIndexWriteChannel="INDEX_WRITE_CHANNEL";
	

	@Output(entityWriteChannel)
	MessageChannel entityWriteChannel();
	
	@Output(kvEntityWriteChannel)
	MessageChannel kvEntityWriteChannel();

	@Output(entityWithoutSysAttrsWriteChannel)
	MessageChannel entityWithoutSysAttrsWriteChannel();

	@Output(createWriteChannel)
	MessageChannel createWriteChannel();

	@Output(appendWriteChannel)
	MessageChannel appendWriteChannel();

	@Output(updateWriteChannel)
	MessageChannel updateWriteChannel();

	@Output(deleteWriteChannel)
	MessageChannel deleteWriteChannel();

	@Output(contextRegistryWriteChannel)
	MessageChannel contextRegistryWriteChannel();
	
	@Output(contextUpdateWriteChannel)
	MessageChannel contextUpdateWriteChannel();
	
	@Output(entityIndexWriteChannel)
	MessageChannel entityIndexWriteChannel();
	
	
}
