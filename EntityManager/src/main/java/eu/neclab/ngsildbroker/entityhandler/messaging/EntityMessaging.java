package eu.neclab.ngsildbroker.entityhandler.messaging;

import java.io.IOException;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment.Strategy;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.serialization.messaging.CollectMessageListener;
import eu.neclab.ngsildbroker.commons.serialization.messaging.MessageCollector;
import io.netty.channel.EventLoopGroup;
import io.quarkus.arc.profile.IfBuildProfile;
import io.quarkus.arc.profile.UnlessBuildProfile;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@Singleton
@UnlessBuildProfile("in-memory")
public class EntityMessaging extends EntityMessagingBase {

	private static final Logger logger = LoggerFactory.getLogger(EntityMessaging.class);
	@Inject
	Vertx vertx;

	@Inject
	ObjectMapper objectMapper;

	private EventLoopGroup executor;

	private MessageCollector collector = new MessageCollector();

	@PostConstruct
	public void setup() {
		this.executor = vertx.getDelegate().nettyEventLoopGroup();
	}

	CollectMessageListener collectListenerRegistry = new CollectMessageListener() {

		@Override
		public void collected(byte[] byteMessage) {
			BaseRequest message;
			try {
				message = objectMapper.readValue(byteMessage, BaseRequest.class);
			} catch (IOException e) {
				logger.error("failed to read sync message", e);
				return;
			}
			baseHandleCsource(message).runSubscriptionOn(executor).subscribe().with(v -> logger.debug("done handling registry"));
		}
	};

	@Incoming(AppConstants.REGISTRY_RETRIEVE_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	public Uni<Void> handleCsource(String byteMessage) {
		collector.collect(byteMessage.getBytes(), collectListenerRegistry);
		return Uni.createFrom().voidItem();
	}

}