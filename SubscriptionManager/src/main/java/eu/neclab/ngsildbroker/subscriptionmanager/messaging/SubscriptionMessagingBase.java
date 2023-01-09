package eu.neclab.ngsildbroker.subscriptionmanager.messaging;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.InternalNotification;
import eu.neclab.ngsildbroker.subscriptionmanager.service.SubscriptionService;
import eu.neclab.ngsildbroker.subscriptionmanager.service.oldSubscriptionService;
import io.smallrye.mutiny.Uni;

public abstract class SubscriptionMessagingBase {
	private static final Logger logger = LoggerFactory.getLogger(SubscriptionMessagingBase.class);
	@Inject
	SubscriptionService subscriptionService;

	public Uni<Void> baseHandleEntity(BaseRequest message) {
		logger.debug("CSource sub manager got called for csource: " + message.getId());
		return subscriptionService.checkSubscriptions(message);
	}

	public Uni<Void> baseHandleInternalNotification(InternalNotification message) {
		return subscriptionService.handleRegistryNotification(message);
	}

	

}
