package eu.neclab.ngsildbroker.historymanager.service;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import io.smallrye.mutiny.Uni;

@ApplicationScoped
public class HistoryKafkaService extends HistoryKafkaServiceBase{

	
	@Incoming(AppConstants.ENTITY_RETRIEVE_CHANNEL)
	public Uni<Void> handleEntity(Message<BaseRequest> message) {
		BaseRequest request = message.getPayload();
		handleBaseMessage(request.getId(), request);
		return Uni.createFrom().nullItem();
	}
}
