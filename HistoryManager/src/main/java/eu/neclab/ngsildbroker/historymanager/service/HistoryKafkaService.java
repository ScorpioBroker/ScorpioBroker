package eu.neclab.ngsildbroker.historymanager.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.AppendHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.HistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.UpdateHistoryEntityRequest;

@Service
@ConditionalOnProperty(prefix = "scorpio.history", name = "autorecording", matchIfMissing = true, havingValue = "active")
public class HistoryKafkaService {

	private static Logger logger = LoggerFactory.getLogger(HistoryKafkaService.class);

	@Autowired
	HistoryService historyService;

	public HistoryKafkaService() {

	}

	@KafkaListener(topics = "${scorpio.topics.entity}", groupId = "history")
	public void handleEntity(@Payload BaseRequest message, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
			@Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timeStamp) throws Exception {
		HistoryEntityRequest request;
		switch (message.getRequestType()) {
		case AppConstants.APPEND_REQUEST:
			logger.debug("Append got called: " + key);
			request = new AppendHistoryEntityRequest(message);
			break;
		case AppConstants.CREATE_REQUEST:
			logger.debug("Create got called: " + key);
			request = new CreateHistoryEntityRequest(message);
			break;
		case AppConstants.UPDATE_REQUEST:
			logger.debug("Update got called: " + key);
			request = new UpdateHistoryEntityRequest(message);
			break;
		case AppConstants.DELETE_REQUEST:
			logger.debug("Delete got called: " + key);
			request = null;
			break;
		default:
			request = null;
			break;
		}
		if (request != null) {
			historyService.handleRequest(request);
		}
	}
}
