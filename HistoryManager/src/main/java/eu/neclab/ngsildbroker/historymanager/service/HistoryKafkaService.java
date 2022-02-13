package eu.neclab.ngsildbroker.historymanager.service;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;

@ApplicationScoped
public class HistoryKafkaService extends HistoryKafkaServiceBase{

	
	@Incoming("entityChannel")
	public void handleEntity(Message<BaseRequest> message) {
		BaseRequest request = message.getPayload();
		handleBaseMessage(request.getId(), request);
	}
}
