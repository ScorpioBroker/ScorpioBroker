package eu.neclab.ngsildbroker.queryhandler.messaging;

import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CSourceBaseRequest;
import eu.neclab.ngsildbroker.commons.serialization.messaging.CollectMessageListener;
import eu.neclab.ngsildbroker.commons.serialization.messaging.MessageCollector;
import eu.neclab.ngsildbroker.queryhandler.services.QueryService;
import io.netty.channel.EventLoopGroup;
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

	private MessageCollector collector = new MessageCollector(this.getClass().getName());

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

	public Uni<Void> baseHandleCsource(CSourceBaseRequest message) {
		logger.debug("query manager got called for csource: " + message.getId());
		return queryService.handleRegistryChange(message);
	}

}
