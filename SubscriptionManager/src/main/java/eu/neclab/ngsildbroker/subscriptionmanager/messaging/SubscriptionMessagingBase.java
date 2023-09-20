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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;

public abstract class SubscriptionMessagingBase {

	private static final Logger logger = LoggerFactory.getLogger(SubscriptionMessagingBase.class);
	@Inject
	SubscriptionService subscriptionService;

	public Uni<Void> baseHandleEntity(BaseRequest message) {
		logger.debug("Subscription sub manager got called for entity: " + message.getId());
		return subscriptionService.checkSubscriptions(message).onFailure().recoverWithUni(t -> {
			logger.debug("Exception Occurred in checkSubscriptions: " + t);
			t.printStackTrace();
			logger.debug(t.getStackTrace().toString());
			return Uni.createFrom().voidItem();
		});
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

}
