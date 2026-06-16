package eu.neclab.ngsildbroker.subscriptionmanager.messaging;

import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CSourceBaseRequest;

import eu.neclab.ngsildbroker.subscriptionmanager.service.SubscriptionService;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;

import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.inject.Inject;

public abstract class SubscriptionMessagingBase {

	private static final Logger logger = LoggerFactory.getLogger(SubscriptionMessagingBase.class);
	@Inject
	SubscriptionService subscriptionService;

	public Uni<Void> baseHandleEntity(BaseRequest message) {

		logger.debug("Subscription event consumed - entity ids: {}, requestType: {}, sendTimestamp: {}",
				message.getIds(), message.getRequestType(), message.getSendTimestamp());
		return subscriptionService.handleBaseRequest(message).onFailure().recoverWithUni(t -> {
			logger.debug("Exception Occurred in checkSubscriptions: ", t);
			return Uni.createFrom().voidItem();
		});

	}

	@Inject
	Vertx vertx;

	@Inject
	ObjectMapper objectMapper;

	public Uni<Void> handleEntityRaw(String byteMessage) {
		logger.debug("Received entity message from messaging channel, size: {} bytes", byteMessage.length());
		BaseRequest baseRequest;
		try {
			baseRequest = objectMapper.readValue(byteMessage, BaseRequest.class);
		} catch (JsonProcessingException e) {
			logger.error("failed to serialize message " + byteMessage, e);
			return Uni.createFrom().voidItem();
		}

		return baseHandleEntity(baseRequest);

	}

	public Uni<Void> handleCsourceRaw(String byteMessage) {
		logger.debug("Received csource message from messaging channel, size: {} bytes", byteMessage.length());
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
		logger.debug("Subscription event consumed - csource id: {}, requestType: {}",
				message.getId(), message.getRequestType());
		return subscriptionService.handleRegistryChange(message).onFailure().recoverWithUni(e -> {
			logger.debug("failed to handle registry entry", e);
			return Uni.createFrom().voidItem();
		});
	}

}
