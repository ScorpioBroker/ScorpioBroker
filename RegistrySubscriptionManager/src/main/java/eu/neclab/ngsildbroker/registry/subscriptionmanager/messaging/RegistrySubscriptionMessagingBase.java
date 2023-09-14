package eu.neclab.ngsildbroker.registry.subscriptionmanager.messaging;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.serialization.messaging.CollectMessageListener;
import eu.neclab.ngsildbroker.commons.serialization.messaging.MessageCollector;
import eu.neclab.ngsildbroker.registry.subscriptionmanager.service.RegistrySubscriptionService;
import io.netty.channel.EventLoopGroup;
import io.quarkus.scheduler.Scheduled;
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

	private EventLoopGroup executor;

	@PostConstruct
	public void setup() {
		System.out.println("regsub");
		this.executor = vertx.getDelegate().nettyEventLoopGroup();
	}

	CollectMessageListener collectListenerRegistry = new CollectMessageListener() {

		@Override
		public void collected(String byteMessage) {
			BaseRequest message;
			try {
				message = objectMapper.readValue(byteMessage, BaseRequest.class);
			} catch (IOException e) {
				logger.error("failed to read sync message", e);
				return;
			}
			baseHandleCsource(message).runSubscriptionOn(executor).subscribe()
					.with(v -> logger.debug("done handling registry"));
		}
	};

	CollectMessageListener collectListenerSubscription = new CollectMessageListener() {

		@Override
		public void collected(String byteMessage) {
			SubscriptionRequest message;
			try {
				message = objectMapper.readValue(byteMessage, SubscriptionRequest.class);
			} catch (IOException e) {
				logger.error("failed to read sync message", e);
				return;
			}
			baseHandleSubscription(message).runSubscriptionOn(executor).subscribe()
					.with(v -> logger.debug("done handling sub"));
		}
	};

	public Uni<Void> handleCsourceRaw(String byteMessage) {
		collector.collect(byteMessage, collectListenerRegistry);
		return Uni.createFrom().voidItem();
	}

	public Uni<Void> handleSubscriptionRaw(String byteMessage) {
		collector.collect(byteMessage, collectListenerSubscription);
		return Uni.createFrom().voidItem();
	}

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

	void purge() {
		collector.purge(30000);
	}
}
