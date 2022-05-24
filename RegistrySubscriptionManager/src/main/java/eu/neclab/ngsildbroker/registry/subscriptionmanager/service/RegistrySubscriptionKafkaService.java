package eu.neclab.ngsildbroker.registry.subscriptionmanager.service;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;

@Singleton
public class RegistrySubscriptionKafkaService {

	private final static Logger logger = LoggerFactory.getLogger(RegistrySubscriptionService.class);

	@Inject
	RegistrySubscriptionService subscriptionService;

	@Incoming(AppConstants.REGISTRY_CHANNEL)
	public Uni<Void> handleCsource(Message<BaseRequest> busMessage) {
		BaseRequest message = busMessage.getPayload();
		IncomingKafkaRecordMetadata metaData = busMessage.getMetadata(IncomingKafkaRecordMetadata.class).orElse(null);
		String key = message.getId();
		final long timestamp;
		if (metaData != null) {
			timestamp = metaData.getTimestamp().toEpochMilli();
		} else {
			timestamp = System.currentTimeMillis();
		}
		switch (message.getRequestType()) {
			case AppConstants.DELETE_ATTRIBUTE_REQUEST:
			case AppConstants.APPEND_REQUEST:
				logger.debug("Append got called: " + key);
				subscriptionService.checkSubscriptionsWithDelta(message, timestamp,
						AppConstants.OPERATION_APPEND_ENTITY);
				break;
			case AppConstants.CREATE_REQUEST:
				logger.debug("Create got called: " + key);
				subscriptionService.checkSubscriptionsWithAbsolute(message, timestamp,
						AppConstants.OPERATION_CREATE_ENTITY);
				break;
			case AppConstants.DELETE_REQUEST:
				logger.debug("Delete got called: " + key);
				subscriptionService.checkSubscriptionsWithAbsolute(message, timestamp,
						AppConstants.OPERATION_DELETE_ENTITY);
				break;
			default:
				break;
		}
		return Uni.createFrom().nullItem();
	}

	@Incoming(AppConstants.INTERNAL_SUBS_CHANNEL)
	public Uni<Void> handleSubscription(Message<SubscriptionRequest> busMessage) {
		SubscriptionRequest message = busMessage.getPayload();
		String key = message.getId();
		switch (message.getRequestType()) {
			case AppConstants.UPDATE_REQUEST:
				logger.debug("Append got called: " + key);
				return subscriptionService.updateInternal(message);
			case AppConstants.CREATE_REQUEST:
				logger.debug("Create got called: " + key);
				return subscriptionService.subscribeInternal(message);
			case AppConstants.DELETE_REQUEST:
				logger.debug("Delete got called: " + key);
				return subscriptionService.unsubscribeInternal(message.getSubscription().getId());
			default:
				return Uni.createFrom().nullItem();
		}
	}
}
