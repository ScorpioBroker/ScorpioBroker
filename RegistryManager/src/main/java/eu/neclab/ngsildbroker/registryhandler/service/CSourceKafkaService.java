package eu.neclab.ngsildbroker.registryhandler.service;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;

@ApplicationScoped
public class CSourceKafkaService extends CSourceKafkaServiceBase {

	@Incoming(AppConstants.ENTITY_CHANNEL)
	public void handleEntity(Message<BaseRequest> message) {
		handleBaseRequest(message.getPayload(), message.getPayload().getId());
	}

}
