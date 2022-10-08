package eu.neclab.ngsildbroker.registry.subscriptionmanager.messaging;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import eu.neclab.ngsildbroker.registry.subscriptionmanager.service.RegistrySubscriptionService;
import io.smallrye.mutiny.Uni;

public abstract class RegistrySubscriptionMessagingBase {

	private final static Logger logger = LoggerFactory.getLogger(RegistrySubscriptionService.class);

	@Inject
	RegistrySubscriptionService subscriptionService;

	public Uni<Void> baseHandleCsource(BaseRequest message, long timestamp) {
		String key = message.getId();
		switch (message.getRequestType()) {
			case AppConstants.DELETE_ATTRIBUTE_REQUEST:
			case AppConstants.APPEND_REQUEST:
				logger.debug("Append got called: " + key);
				subscriptionService.checkSubscriptionsWithDelta(message, timestamp,
						AppConstants.OPERATION_APPEND_ENTITY);
				break;
			case AppConstants.CREATE_REQUEST:
				logger.debug("Create got called: " + key);
				subscriptionService.checkSubscriptionsWithAbsolute(message, timestamp,
						AppConstants.OPERATION_CREATE_ENTITY);
				break;
			case AppConstants.DELETE_REQUEST:
				logger.debug("Delete got called: " + key);
				subscriptionService.checkSubscriptionsWithAbsolute(message, timestamp,
						AppConstants.OPERATION_DELETE_ENTITY);
				break;
			default:
				break;
		}
		return Uni.createFrom().nullItem();
	}

	public Uni<Void> baseHandleSubscription(SubscriptionRequest message) {
		String key = message.getId();
		switch (message.getRequestType()) {
			case AppConstants.UPDATE_REQUEST:
				logger.debug("Append got called: " + key);
				return subscriptionService.updateInternal(message);
			case AppConstants.CREATE_REQUEST:
				logger.debug("Create got called: " + key);
				return subscriptionService.subscribeInternal(message);
			case AppConstants.DELETE_REQUEST:
				logger.debug("Delete got called: " + key);
				return subscriptionService.unsubscribeInternal(message.getSubscription().getId());
			default:
				return Uni.createFrom().nullItem();
		}
	}
}
