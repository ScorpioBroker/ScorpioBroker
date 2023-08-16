package eu.neclab.ngsildbroker.registry.subscriptionmanager.messaging;

import jakarta.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.SubscriptionRequest;
import eu.neclab.ngsildbroker.registry.subscriptionmanager.service.RegistrySubscriptionService;

import io.smallrye.mutiny.Uni;

public abstract class RegistrySubscriptionMessagingBase {

	private final static Logger logger = LoggerFactory.getLogger(RegistrySubscriptionMessagingBase.class);

	@Inject
	RegistrySubscriptionService subscriptionService;

	public Uni<Void> baseHandleCsource(BaseRequest message) {
		logger.debug("CSource sub manager got called for csource: " + message.getId());
		return subscriptionService.checkSubscriptions(message).onFailure().recoverWithUni(e -> {
			logger.debug("failed to handle registry entry", e);
			return Uni.createFrom().voidItem();
		});
	}

	public Uni<Void> baseHandleSubscription(SubscriptionRequest message) {
		logger.debug("CSource sub manager got called for internal sub: " + message.getId());
		return subscriptionService.handleInternalSubscription(message).onFailure().recoverWithUni(e -> {
			logger.debug("failed to handle registry entry", e);
			return Uni.createFrom().voidItem();
		});
	}
}
