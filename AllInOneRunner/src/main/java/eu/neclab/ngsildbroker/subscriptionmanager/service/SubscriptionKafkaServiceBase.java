package eu.neclab.ngsildbroker.subscriptionmanager.service;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.InternalNotification;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;

public abstract class SubscriptionKafkaServiceBase {

	private final static Logger logger = LoggerFactory.getLogger(SubscriptionService.class);
	
	@Inject
	SubscriptionService subscriptionService;

	public void handleBaseRequestEntity(BaseRequest message, String key, long timeStamp) {
		switch (message.getRequestType()) {
		case AppConstants.APPEND_REQUEST:
			logger.debug("Append got called: " + key);
			subscriptionService.checkSubscriptionsWithDelta(message, timeStamp, AppConstants.OPERATION_APPEND_ENTITY);
			break;
		case AppConstants.CREATE_REQUEST:
			logger.debug("Create got called: " + key);
			subscriptionService.checkSubscriptionsWithAbsolute(message, timeStamp,
					AppConstants.OPERATION_CREATE_ENTITY);
			break;
		case AppConstants.UPDATE_REQUEST:
			logger.debug("Update got called: " + key);
			subscriptionService.checkSubscriptionsWithDelta(message, timeStamp, AppConstants.OPERATION_UPDATE_ENTITY);
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

	public void handleBaseRequestInternalNotification(InternalNotification message) {
		subscriptionService.handleRegistryNotification(message);
	}
}
