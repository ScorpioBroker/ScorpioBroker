package eu.neclab.ngsildbroker.subscriptionmanager.messaging;

import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.io.IOException;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment.Strategy;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.AliveAnnouncement;
import eu.neclab.ngsildbroker.commons.datatypes.SyncMessage;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BatchRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.InternalNotification;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.serialization.messaging.CollectMessageListener;
import eu.neclab.ngsildbroker.commons.serialization.messaging.MessageCollector;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import io.netty.channel.EventLoopGroup;
import io.quarkus.arc.profile.IfBuildProfile;
import io.quarkus.arc.profile.UnlessBuildProfile;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;

@Singleton
@IfBuildProfile("in-memory")
public class SubscriptionMessagingInMemory extends SubscriptionMessagingBase {

	private static final Logger logger = LoggerFactory.getLogger(SubscriptionMessagingInMemory.class);
	private MessageCollector collector = new MessageCollector();

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
		public void collected(byte[] byteMessage) {
			BaseRequest message;
			try {
				message = objectMapper.readValue(byteMessage, BaseRequest.class);
			} catch (IOException e) {
				logger.error("failed to read entity", e);
				return;
			}
			baseHandleEntity(message).runSubscriptionOn(executor).subscribe()
					.with(v -> logger.debug("done handling entity"));
			;
		}
	};

	CollectMessageListener collectListenerBatchEntity = new CollectMessageListener() {

		@Override
		public void collected(byte[] byteMessage) {
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
		public void collected(byte[] byteMessage) {
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

	@Incoming(AppConstants.ENTITY_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	public Uni<Void> handleEntity(byte[] byteMessage) {
		collector.collect(byteMessage, collectListenerEntity);
		return Uni.createFrom().voidItem();
	}

	@Incoming(AppConstants.INTERNAL_NOTIFICATION_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	public Uni<Void> handleInternalNotification(byte[] byteMessage) {
		collector.collect(byteMessage, collectListenerINotification);
		return Uni.createFrom().voidItem();
	}

	@Incoming(AppConstants.ENTITY_BATCH_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	public Uni<Void> handleBatchEntities(byte[] byteMessage) {
		collector.collect(byteMessage, collectListenerBatchEntity);
		return Uni.createFrom().voidItem();
	}
}
