package eu.neclab.ngsildbroker.registry.subscriptionmanager.messaging;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.neclab.ngsildbroker.commons.datatypes.requests.CSourceBaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.SubscriptionRequest;
import eu.neclab.ngsildbroker.registry.subscriptionmanager.service.RegistrySubscriptionService;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;
import jakarta.inject.Inject;

public abstract class RegistrySubscriptionMessagingBase {

	private final static Logger logger = LoggerFactory.getLogger(RegistrySubscriptionMessagingBase.class);

	@Inject
	RegistrySubscriptionService subscriptionService;

	

	@Inject
	Vertx vertx;

	@Inject
	ObjectMapper objectMapper;

	public Uni<Void> handleCsourceRaw(String byteMessage) {
		CSourceBaseRequest message;
		try {
			message = objectMapper.readValue(byteMessage, CSourceBaseRequest.class);
		} catch (IOException e) {
			logger.error("failed to read sync message", e);
			return Uni.createFrom().voidItem();
		}
		return baseHandleCsource(message);
	}


	public Uni<Void> baseHandleCsource(CSourceBaseRequest message) {
		logger.debug("CSource sub manager got called for csource: " + message.getId());
		return subscriptionService.handleRegistryChange(message).onFailure().recoverWithUni(e -> {
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
