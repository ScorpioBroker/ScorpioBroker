package eu.neclab.ngsildbroker.subscriptionmanager.service;

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
public class SubscriptionKafkaService {

	private final static Logger logger = LoggerFactory.getLogger(SubscriptionService.class);
	@Autowired
	SubscriptionService subscriptionService;

	@KafkaListener(topics = "${entity.append.topic}")
	public void handleAppend(@Payload BaseRequest message, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
			@Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timeStamp) {
		logger.debug("Append got called: " + key);
		subscriptionService.checkSubscriptionsWithDelta(message, timeStamp, AppConstants.OPERATION_APPEND_ENTITY);
	}

	@KafkaListener(topics = "${entity.update.topic}")
	public void handleUpdate(@Payload BaseRequest message, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
			@Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timeStamp) {
		logger.debug("Update got called: " + key);
		subscriptionService.checkSubscriptionsWithDelta(message, timeStamp, AppConstants.OPERATION_APPEND_ENTITY);
	}

	@KafkaListener(topics = "${entity.delete.topic}")
	public void handleDelete(@Payload BaseRequest message, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
			@Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timeStamp, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
		logger.debug("Create got called: " + key);
		System.err.println(key + " : " + topic + " : ");
		subscriptionService.checkSubscriptionsWithAbsolute(message, timeStamp, AppConstants.OPERATION_DELETE_ENTITY);
	}

	@KafkaListener(topics = "${entity.create.topic}")
	public void handleCreate(@Payload BaseRequest message, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
			@Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timeStamp) {
		logger.debug("Create got called: " + key);
		subscriptionService.checkSubscriptionsWithAbsolute(message, timeStamp, AppConstants.OPERATION_CREATE_ENTITY);
	}

//	@KafkaListener(topics = "${csource.create.topic}")
//	public void handleCSourceCreate(@Payload BaseRequest message, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
//			@Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timeStamp) {
//
//	}
//
//	@KafkaListener(topics = "${csource.append.topic}")
//	public void handleCSourceAppend(@Payload BaseRequest message, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
//			@Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timeStamp) {
//
//	}
//
//	@KafkaListener(topics = "${csource.update.topic}")
//	public void handleCSourceUpdate(@Payload BaseRequest message, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
//			@Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timeStamp) {
//
//	}
//
//	@KafkaListener(topics = "${csource.delete.topic}")
//	public void handleCSourceDelete(@Payload BaseRequest message, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
//			@Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timeStamp) {
//
//	}

}
