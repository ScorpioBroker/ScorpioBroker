package eu.neclab.ngsildbroker.historyquerymanager.messaging;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CSourceBaseRequest;
import eu.neclab.ngsildbroker.commons.serialization.messaging.CollectMessageListener;
import eu.neclab.ngsildbroker.commons.serialization.messaging.MessageCollector;
import eu.neclab.ngsildbroker.historyquerymanager.service.HistoryQueryService;
import io.netty.channel.EventLoopGroup;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;

public abstract class HistoryMessagingBase {

	private static Logger logger = LoggerFactory.getLogger(HistoryMessagingBase.class);

	@Inject
	HistoryQueryService historyService;

	@Inject
	Vertx vertx;

	@Inject
	ObjectMapper objectMapper;





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
		logger.debug("history query manager got called for csource: " + message.getId());
		return historyService.handleRegistryChange(message);
	}


}
