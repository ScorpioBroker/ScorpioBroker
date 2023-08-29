package eu.neclab.ngsildbroker.registry.subscriptionmanager.messaging;

import java.io.IOException;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment.Strategy;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.SubscriptionRequest;
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
@IfBuildProfile("in-memory")
public class RegistrySubscriptionMessagingInMemory extends RegistrySubscriptionMessagingBase {

	private static final Logger logger = LoggerFactory.getLogger(RegistrySubscriptionMessagingInMemory.class);
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
			baseHandleCsource(message).runSubscriptionOn(executor).subscribe();
		}
	};
	
	CollectMessageListener collectListenerSubscription = new CollectMessageListener() {

		@Override
		public void collected(byte[] byteMessage) {
			SubscriptionRequest message;
			try {
				message = objectMapper.readValue(byteMessage, SubscriptionRequest.class);
			} catch (IOException e) {
				logger.error("failed to read sync message", e);
				return;
			}
			baseHandleSubscription(message).runSubscriptionOn(executor).subscribe();
		}
	};


	


	@Incoming(AppConstants.REGISTRY_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	public Uni<Void> handleCsource(byte[] byteMessage) {
		collector.collect(byteMessage, collectListenerRegistry);
		return Uni.createFrom().voidItem();
	}

	@Incoming(AppConstants.INTERNAL_SUBS_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	public Uni<Void> handleSubscription(byte[] byteMessage) {
		collector.collect(byteMessage, collectListenerSubscription);
		return Uni.createFrom().voidItem();
	}
}
