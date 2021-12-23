package eu.neclab.ngsildbroker.registry.subscriptionmanager.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BaseRequest;

@Service
public class RegistrySubscriptionKafkaService {

	private final static Logger logger = LoggerFactory.getLogger(RegistrySubscriptionService.class);
	@Autowired
	RegistrySubscriptionService subscriptionService;

	@KafkaListener(topics = "${csource.append.topic}")
	public void handleAppend(@Payload BaseRequest message, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
			@Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timeStamp) {
		logger.debug("Append got called: " + key);
		subscriptionService.checkSubscriptionsWithDelta(message, timeStamp, AppConstants.OPERATION_APPEND_ENTITY);
	}

	@KafkaListener(topics = "${csource.delete.topic}")
	public void handleDelete(@Payload BaseRequest message, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
			@Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timeStamp) {
		logger.debug("Create got called: " + key);
		subscriptionService.checkSubscriptionsWithAbsolute(message, timeStamp, AppConstants.OPERATION_DELETE_ENTITY);
	}

	@KafkaListener(topics = "${csource.create.topic}")
	public void handleCreate(@Payload BaseRequest message, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
			@Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timeStamp) {
		logger.debug("Create got called: " + key);
		subscriptionService.checkSubscriptionsWithAbsolute(message, timeStamp, AppConstants.OPERATION_CREATE_ENTITY);
	}

}
