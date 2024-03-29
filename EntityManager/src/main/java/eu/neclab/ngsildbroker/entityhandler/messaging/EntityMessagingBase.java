package eu.neclab.ngsildbroker.entityhandler.messaging;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.serialization.messaging.CollectMessageListener;
import eu.neclab.ngsildbroker.commons.serialization.messaging.MessageCollector;
import eu.neclab.ngsildbroker.entityhandler.services.EntityService;
import io.netty.channel.EventLoopGroup;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;

public abstract class EntityMessagingBase {

	private final static Logger logger = LoggerFactory.getLogger(EntityMessagingBase.class);

	@Inject
	EntityService entityService;

	@Inject
	Vertx vertx;

	@Inject
	ObjectMapper objectMapper;

	private EventLoopGroup executor;

	private MessageCollector collector = new MessageCollector(this.getClass().getName());

	@PostConstruct
	public void setup() {
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

	public Uni<Void> handleCsourceRaw(String byteMessage) {
		collector.collect(byteMessage, collectListenerRegistry);
		return Uni.createFrom().voidItem();
	}

	public Uni<Void> baseHandleCsource(BaseRequest message) {
		logger.debug("entity manager got called for csource: " + message.getId());
		return entityService.handleRegistryChange(message);
	}

	void purge() {
		collector.purge(30000);
	}

}
