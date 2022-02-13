package eu.neclab.ngsildbroker.registry.subscriptionmanager.service;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;

public abstract class RegistrySubscriptionKafkaServiceBase {

	private final static Logger logger = LoggerFactory.getLogger(RegistrySubscriptionService.class);
	@Inject
	RegistrySubscriptionService subscriptionService;

	public void handleBaseRequestRegistry(BaseRequest message, String key, long timeStamp) {
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

	public void handleBaseRequestSubscription(SubscriptionRequest message, String key) {
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
