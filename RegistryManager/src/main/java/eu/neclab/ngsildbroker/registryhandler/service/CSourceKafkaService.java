package eu.neclab.ngsildbroker.registryhandler.service;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import io.vertx.core.eventbus.Message;

@Singleton
public class CSourceKafkaService {
	private static final Logger logger = LoggerFactory.getLogger(CSourceKafkaService.class);

	@Inject
	CSourceService cSourceService;

	@Incoming(AppConstants.ENTITY_RETRIEVE_CHANNEL)
	public void handleEntity(Message<BaseRequest> mutinyMessage) {
		BaseRequest message = mutinyMessage.body();
		switch (message.getRequestType()) {
			case AppConstants.DELETE_REQUEST:
				cSourceService.handleEntityDelete(message);
				break;
			case AppConstants.UPDATE_REQUEST:
			case AppConstants.CREATE_REQUEST:
			case AppConstants.DELETE_ATTRIBUTE_REQUEST:
			case AppConstants.APPEND_REQUEST:
				cSourceService.handleEntityCreateOrUpdate(message);
				break;
			default:
				break;
		}
	}

}
