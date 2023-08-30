package eu.neclab.ngsildbroker.historyentitymanager.messaging;

import java.io.IOException;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment.Strategy;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BatchRequest;
import eu.neclab.ngsildbroker.commons.serialization.messaging.CollectMessageListener;
import eu.neclab.ngsildbroker.commons.serialization.messaging.MessageCollector;
import io.netty.channel.EventLoopGroup;
import io.quarkus.arc.profile.IfBuildProfile;
import io.quarkus.arc.profile.UnlessBuildProfile;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.common.annotation.RunOnVirtualThread;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
@IfBuildProfile("in-memory")
public class HistoryMessagingInMemory extends HistoryMessagingBase {

	private static final Logger logger = LoggerFactory.getLogger(HistoryMessagingInMemory.class);
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
		public void collected(String byteMessage) {
			BaseRequest message;
			try {
				message = objectMapper.readValue(byteMessage, BaseRequest.class);
			} catch (IOException e) {
				logger.error("failed to read sync message", e);
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
				logger.error("failed to read sync message", e);
				return;
			}
			baseHandleBatch(message).runSubscriptionOn(executor).subscribe()
					.with(v -> logger.debug("done handling batch"));
		}
	};

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

	@Incoming(AppConstants.REGISTRY_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	public Uni<Void> handleCsource(String byteMessage) {
		collector.collect(byteMessage, collectListenerRegistry);
		return Uni.createFrom().voidItem();
	}

	@Incoming(AppConstants.ENTITY_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	public Uni<Void> handleEntity(String byteMessage) {
		collector.collect(byteMessage, collectListenerEntity);
		return Uni.createFrom().voidItem();
	}

	@Incoming(AppConstants.ENTITY_BATCH_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	public Uni<Void> handleBatchEntities(String byteMessage) {
		collector.collect(byteMessage, collectListenerBatchEntity);
		return Uni.createFrom().voidItem();
	}

	@Scheduled(every = "5s")
	@RunOnVirtualThread
	Uni<Void> checkBuffer() {
		return super.checkBuffer();
	}
}
