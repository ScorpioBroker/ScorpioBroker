package eu.neclab.ngsildbroker.subscriptionmanager.messaging;

import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BatchRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.InternalNotification;
import eu.neclab.ngsildbroker.commons.serialization.messaging.CollectMessageListener;
import eu.neclab.ngsildbroker.commons.serialization.messaging.MessageCollector;
import eu.neclab.ngsildbroker.subscriptionmanager.service.SubscriptionService;
import io.netty.channel.EventLoopGroup;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;

import java.io.IOException;
import java.util.ArrayList;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;

public abstract class SubscriptionMessagingBase {

	private static final Logger logger = LoggerFactory.getLogger(SubscriptionMessagingBase.class);
	@Inject
	SubscriptionService subscriptionService;

	@ConfigProperty(name = "scorpio.subscriptions.collectinterval", defaultValue = "-1")
	int collectInterval;

	@ConfigProperty(name = "scorpio.subscriptions.collectmaxtime", defaultValue = "1000")
	int collectMaxTime;

	long lastMessage = System.currentTimeMillis();
	long lastSent = System.currentTimeMillis();

	ArrayList<BaseRequest> requestStore = new ArrayList<>();

	public Uni<Void> baseHandleEntity(BaseRequest message) {
		if (collectInterval == -1) {
			logger.debug("Subscription sub manager got called for entity: " + message.getId());
			return subscriptionService.checkSubscriptions(message).onFailure().recoverWithUni(t -> {
				logger.debug("Exception Occurred in checkSubscriptions: " + t);
				t.printStackTrace();
				logger.debug(t.getStackTrace().toString());
				return Uni.createFrom().voidItem();
			});
		} else {
			requestStore.add(message);
			lastMessage = System.currentTimeMillis();
			return Uni.createFrom().voidItem();
		}
	}

	public Uni<Void> baseHandleBatchEntities(BatchRequest message) {
		logger.debug("Subscription sub manager got called for batch request: " + message.getEntityIds());
		return subscriptionService.checkSubscriptions(message).onFailure().recoverWithUni(t -> {
			logger.debug("Exception Occurred in checkSubscriptions: " + t);
			return Uni.createFrom().voidItem();
		});
	}

	public Uni<Void> baseHandleInternalNotification(InternalNotification message) {
		return subscriptionService.handleRegistryNotification(message);
	}

	private MessageCollector collector = new MessageCollector(this.getClass().getName());

	@Inject
	Vertx vertx;

	@Inject
	ObjectMapper objectMapper;

	private EventLoopGroup executor;

	@PostConstruct
	public void setup() {
		this.executor = vertx.getDelegate().nettyEventLoopGroup();
	}

	CollectMessageListener collectListenerEntity = new CollectMessageListener() {

		@Override
		public void collected(String byteMessage) {
			BaseRequest message;
			try {
				message = objectMapper.readValue(byteMessage, BaseRequest.class);
			} catch (IOException e) {
				logger.error("failed to read entity", e);
				return;
			}
			baseHandleEntity(message).runSubscriptionOn(executor).subscribe()
					.with(v -> logger.debug("done handling entity"));
		}
	};

	CollectMessageListener collectListenerBatchEntity = new CollectMessageListener() {

		@Override
		public void collected(String byteMessage) {
			logger.info("receiving entities");
//			logger.info(byteMessage);
			BatchRequest message;
			try {
				message = objectMapper.readValue(byteMessage, BatchRequest.class);
			} catch (IOException e) {
				logger.error("failed to read batch entity", e);
				return;
			}
			baseHandleBatchEntities(message).runSubscriptionOn(executor).subscribe()
					.with(v -> logger.debug("done handling batch"));
		}
	};

	CollectMessageListener collectListenerINotification = new CollectMessageListener() {

		@Override
		public void collected(String byteMessage) {
			InternalNotification message;
			try {
				message = objectMapper.readValue(byteMessage, InternalNotification.class);
			} catch (IOException e) {
				logger.error("failed to read notification message", e);
				return;
			}
			baseHandleInternalNotification(message).runSubscriptionOn(executor).subscribe()
					.with(v -> logger.debug("done handling notification"));
		}
	};

	public Uni<Void> handleEntityRaw(String byteMessage) {
		collector.collect(byteMessage, collectListenerEntity);
		return Uni.createFrom().voidItem();
	}

	public Uni<Void> handleInternalNotificationRaw(String byteMessage) {
		collector.collect(byteMessage, collectListenerINotification);
		return Uni.createFrom().voidItem();
	}

	public Uni<Void> handleBatchEntitiesRaw(String byteMessage) {
		collector.collect(byteMessage, collectListenerBatchEntity);
		return Uni.createFrom().voidItem();
	}

	void purge() {
		collector.purge(30000);
	}

	Uni<Void> emptyQueue() {

		return Uni.createFrom().voidItem();
	}

}
