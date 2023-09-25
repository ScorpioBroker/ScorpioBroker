package eu.neclab.ngsildbroker.queryhandler.messaging;

import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.serialization.messaging.CollectMessageListener;
import eu.neclab.ngsildbroker.commons.serialization.messaging.MessageCollector;
import eu.neclab.ngsildbroker.queryhandler.services.QueryService;
import io.netty.channel.EventLoopGroup;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;

public abstract class QueryManagerMessagingBase {

	private static Logger logger = LoggerFactory.getLogger(QueryManagerMessagingBase.class);

	@Inject
	QueryService queryService;

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
		logger.debug("query manager got called for csource: " + message.getId());
		return queryService.handleRegistryChange(message);
	}

	void purge() {
		collector.purge(30000);
	}
}
