package eu.neclab.ngsildbroker.subscriptionmanager.messaging;

import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CSourceBaseRequest;

import eu.neclab.ngsildbroker.subscriptionmanager.service.SubscriptionService;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;

import java.io.IOException;
import java.util.ArrayList;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.inject.Inject;

public abstract class SubscriptionMessagingBase {

	private static final Logger logger = LoggerFactory.getLogger(SubscriptionMessagingBase.class);
	@Inject
	SubscriptionService subscriptionService;

	@ConfigProperty(name = "scorpio.subscriptions.collectinterval", defaultValue = "-1")
	int collectInterval;

	@ConfigProperty(name = "scorpio.subscriptions.collectmaxtime", defaultValue = "1000")
	int collectMaxTime;

	long lastMessage = System.currentTimeMillis();
	long lastSent = System.currentTimeMillis();
	

	ArrayList<BaseRequest> requestStore = new ArrayList<>();

	public Uni<Void> baseHandleEntity(BaseRequest message) {
		if (collectInterval == -1) {
			logger.debug("Subscription sub manager got called for entity: " + message.getIds());
			return subscriptionService.handleBaseRequest(message).onFailure().recoverWithUni(t -> {
				logger.debug("Exception Occurred in checkSubscriptions: " + t);
				t.printStackTrace();
				logger.debug(t.getStackTrace().toString());
				return Uni.createFrom().voidItem();
			});
		} else {
			requestStore.add(message);
			lastMessage = System.currentTimeMillis();
			return Uni.createFrom().voidItem();
		}
	}

	
	@Inject
	Vertx vertx;

	@Inject
	ObjectMapper objectMapper;

	public Uni<Void> handleEntityRaw(String byteMessage) {
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
		logger.debug("CSource sub manager got called for csource: " + message.getId());
		return subscriptionService.handleRegistryChange(message).onFailure().recoverWithUni(e -> {
			logger.debug("failed to handle registry entry", e);
			return Uni.createFrom().voidItem();
		});
	}

}
