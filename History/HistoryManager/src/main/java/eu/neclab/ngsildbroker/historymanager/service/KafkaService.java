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

import eu.neclab.ngsildbroker.commons.datatypes.AppendHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.CreateHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.UpdateHistoryEntityRequest;

@Service
@ConditionalOnProperty(name = "scorpio.history.autorecording", matchIfMissing = true, havingValue = "active")
public class KafkaService {

	private static Logger logger = LoggerFactory.getLogger(KafkaService.class);

	@Autowired
	HistoryService historyService;

	@KafkaListener(topics = "${entity.create.topic}")
	public void handleEntityCreate(@Payload BaseRequest message, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key)
			throws Exception {
		logger.trace("Listener handleEntityCreate...");
		logger.debug("Received message: " + message);
		CreateHistoryEntityRequest request = new CreateHistoryEntityRequest(message);
		historyService.handleRequest(request);
	}

	@KafkaListener(topics = "${entity.append.topic}")
	public void handleEntityAppend(@Payload BaseRequest message, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key)
			throws Exception {
		logger.trace("Listener handleEntityAppend...");
		logger.debug("Received message: " + message);
		AppendHistoryEntityRequest request = new AppendHistoryEntityRequest(message);
		historyService.handleRequest(request);

	}

	@KafkaListener(topics = "${entity.update.topic}")
	public void handleEntityUpdate(@Payload BaseRequest message, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key)
			throws Exception {
		logger.trace("Listener handleEntityUpdate...");
		logger.debug("Received message: " + message);
		UpdateHistoryEntityRequest request = new UpdateHistoryEntityRequest(message);
		historyService.handleRequest(request);
	}

	

}
