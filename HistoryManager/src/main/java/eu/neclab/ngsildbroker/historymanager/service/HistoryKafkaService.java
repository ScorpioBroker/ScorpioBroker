package eu.neclab.ngsildbroker.historymanager.service;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.AppendHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.HistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.UpdateHistoryEntityRequest;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.eventbus.Message;

@Singleton
public class HistoryKafkaService {

	private static Logger logger = LoggerFactory.getLogger(HistoryKafkaService.class);

	@Inject
	HistoryService historyService;

	public HistoryKafkaService() {

	}

	@Incoming(AppConstants.ENTITY_RETRIEVE_CHANNEL)
	public Uni<Void> handleEntity(Message<BaseRequest> message) {
		HistoryEntityRequest request;
		try {
			switch (message.body().getRequestType()) {
				case AppConstants.APPEND_REQUEST:
					logger.debug("Append got called: " + message.body().getId());
					request = new AppendHistoryEntityRequest(message.body());
					break;
				case AppConstants.CREATE_REQUEST:
					logger.debug("Create got called: " + message.body().getId());
					request = new CreateHistoryEntityRequest(message.body());
					break;
				case AppConstants.UPDATE_REQUEST:
					logger.debug("Update got called: " + message.body().getId());
					request = new UpdateHistoryEntityRequest(message.body());
					break;
				case AppConstants.DELETE_REQUEST:
					logger.debug("Delete got called: " + message.body().getId());
					request = null;
					break;
				default:
					request = null;
					break;
			}
			if (request != null) {
				historyService.handleRequest(request);
			}
		} catch (Exception e) {
			logger.error("Internal history recording failed", e.getMessage());
		}
		return Uni.createFrom().nullItem();
	}
}
