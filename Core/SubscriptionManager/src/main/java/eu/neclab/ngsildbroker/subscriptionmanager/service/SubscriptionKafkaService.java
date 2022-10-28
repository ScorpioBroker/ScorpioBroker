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
import eu.neclab.ngsildbroker.commons.datatypes.InternalNotification;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;

@Service
public class SubscriptionKafkaService {

	private final static Logger logger = LoggerFactory.getLogger(SubscriptionService.class);
	@Autowired
	SubscriptionService subscriptionService;

	@KafkaListener(topics = "${scorpio.topics.entity}", groupId = "${random.uuid}")
	public void handleEntity(@Payload BaseRequest message, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
			@Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timeStamp) {
		switch (message.getRequestType()) {
			case AppConstants.APPEND_REQUEST:
				logger.debug("Append got called: " + key);
				subscriptionService.checkSubscriptionsWithDelta(message, timeStamp,
						AppConstants.OPERATION_APPEND_ENTITY);
				break;
			case AppConstants.CREATE_REQUEST:
				logger.debug("Create got called: " + key);
				subscriptionService.checkSubscriptionsWithAbsolute(message, timeStamp,
						AppConstants.OPERATION_CREATE_ENTITY);
				break;
			case AppConstants.UPDATE_REQUEST:
				logger.debug("Update got called: " + key);
				subscriptionService.checkSubscriptionsWithDelta(message, timeStamp,
						AppConstants.OPERATION_UPDATE_ENTITY);
				break;
			case AppConstants.DELETE_REQUEST:
				logger.debug("Delete got called: " + key);
				subscriptionService.checkSubscriptionsWithAbsolute(message, timeStamp,
						AppConstants.OPERATION_DELETE_ENTITY);
				break;
			case AppConstants.BATCH_ERROR_REQUEST:
				logger.debug("Finalizing batch " + key);
				subscriptionService.addFail(message.getBatchInfo());
				break;
			default:
				break;
		}
	}

	@KafkaListener(topics = "${scorpio.topics.internalnotification}", groupId = "${random.uuid}")
	public void handleInternalNotification(@Payload InternalNotification message,
			@Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
			@Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timeStamp) {
		subscriptionService.handleRegistryNotification(message);
	}
}
