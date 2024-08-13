package eu.neclab.ngsildbroker.registry.subscriptionmanager.messaging;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CSourceBaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.serialization.messaging.CollectMessageListener;
import eu.neclab.ngsildbroker.commons.serialization.messaging.MessageCollector;
import eu.neclab.ngsildbroker.registry.subscriptionmanager.service.RegistrySubscriptionService;
import io.netty.channel.EventLoopGroup;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;

public abstract class RegistrySubscriptionMessagingBase {

	private final static Logger logger = LoggerFactory.getLogger(RegistrySubscriptionMessagingBase.class);

	@Inject
	RegistrySubscriptionService subscriptionService;

	private MessageCollector collector = new MessageCollector(this.getClass().getName());

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

	public Uni<Void> handleSubscriptionRaw(String byteMessage) {
		SubscriptionRequest message;
		try {
			message = objectMapper.readValue(byteMessage, SubscriptionRequest.class);
		} catch (IOException e) {
			logger.error(byteMessage);
			logger.error("failed to read internal subscription", e);
			return Uni.createFrom().voidItem();
		}
		return baseHandleSubscription(message);

	}

	public Uni<Void> baseHandleCsource(CSourceBaseRequest message) {
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
