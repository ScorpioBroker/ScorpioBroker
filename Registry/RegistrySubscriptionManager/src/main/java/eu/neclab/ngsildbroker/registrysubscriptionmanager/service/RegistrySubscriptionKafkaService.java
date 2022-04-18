package eu.neclab.ngsildbroker.registrysubscriptionmanager.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;

@Service
public class RegistrySubscriptionKafkaService {

	private final static Logger logger = LoggerFactory.getLogger(RegistrySubscriptionService.class);
	@Autowired
	RegistrySubscriptionService subscriptionService;

	@KafkaListener(topics = "${scorpio.topics.registry}", groupId = "${random.uuid}")
	public void handleCsource(@Payload BaseRequest message, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
			@Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timeStamp) {
		switch (message.getRequestType()) {
		case AppConstants.DELETE_ATTRIBUTE_REQUEST:
		case AppConstants.APPEND_REQUEST:
			logger.debug("Append got called: " + key);
			subscriptionService.checkSubscriptionsWithDelta(message, timeStamp, AppConstants.OPERATION_APPEND_ENTITY);
			break;
		case AppConstants.CREATE_REQUEST:
			logger.debug("Create got called: " + key);
			subscriptionService.checkSubscriptionsWithAbsolute(message, timeStamp,
					AppConstants.OPERATION_CREATE_ENTITY);
			break;
		case AppConstants.DELETE_REQUEST:
			logger.debug("Delete got called: " + key);
			subscriptionService.checkSubscriptionsWithAbsolute(message, timeStamp,
					AppConstants.OPERATION_DELETE_ENTITY);
			break;
		default:
			break;
		}
	}

	@KafkaListener(topics = "${scorpio.topics.internalregsub}", groupId = "${random.uuid}")
	public void handleSubscription(@Payload SubscriptionRequest message,
			@Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
			@Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timeStamp) {
		switch (message.getRequestType()) {
		case AppConstants.UPDATE_REQUEST:
			logger.debug("Append got called: " + key);
			subscriptionService.updateInternal(message);
			break;
		case AppConstants.CREATE_REQUEST:
			logger.debug("Create got called: " + key);
			subscriptionService.subscribeInternal(message);
			break;
		case AppConstants.DELETE_REQUEST:
			logger.debug("Delete got called: " + key);
			subscriptionService.unsubscribeInternal(message.getSubscription().getId());
			break;
		default:
			break;
		}
	}
}
