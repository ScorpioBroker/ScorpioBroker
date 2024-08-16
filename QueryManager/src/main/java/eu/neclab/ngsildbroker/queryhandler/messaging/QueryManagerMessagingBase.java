package eu.neclab.ngsildbroker.queryhandler.messaging;

import jakarta.inject.Inject;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.neclab.ngsildbroker.commons.datatypes.requests.CSourceBaseRequest;
import eu.neclab.ngsildbroker.queryhandler.services.QueryService;
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
