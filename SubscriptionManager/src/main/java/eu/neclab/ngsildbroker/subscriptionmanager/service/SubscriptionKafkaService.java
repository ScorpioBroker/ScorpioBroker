package eu.neclab.ngsildbroker.subscriptionmanager.service;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.InternalNotification;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;

@Singleton
public class SubscriptionKafkaService {
	private static final Logger logger = LoggerFactory.getLogger(SubscriptionKafkaService.class);
	private ThreadPoolExecutor entityExecutor = new ThreadPoolExecutor(10, 50, 1, TimeUnit.MINUTES,
			new LinkedBlockingQueue<Runnable>());
	private ThreadPoolExecutor notificationExecutor = new ThreadPoolExecutor(10, 50, 1, TimeUnit.MINUTES,
			new LinkedBlockingQueue<Runnable>());

	@Inject
	SubscriptionService subscriptionService;

	@Incoming(AppConstants.ENTITY_RETRIEVE_CHANNEL)
	public Uni<Void> handleEntity(Message<BaseRequest> message) {
		IncomingKafkaRecordMetadata<String, Object> metaData = message.getMetadata(IncomingKafkaRecordMetadata.class)
				.orElse(null);
		final long timestamp;
		if (metaData != null) {
			timestamp = metaData.getTimestamp().toEpochMilli();
		} else {
			timestamp = System.currentTimeMillis();
		}
		BaseRequest payload = message.getPayload();
		entityExecutor.execute(new Runnable() {
			@Override
			public void run() {
				handleBaseRequestEntity(payload, payload.getId(), timestamp);
			}
		});
		return Uni.createFrom().nullItem();
	}

	@Incoming(AppConstants.INTERNAL_RETRIEVE_NOTIFICATION_CHANNEL)
	public Uni<Void> handleInternalNotification(Message<InternalNotification> message) {
		notificationExecutor.execute(new Runnable() {
			@Override
			public void run() {
				handleBaseRequestInternalNotification(message.getPayload());
			}
		});
		return Uni.createFrom().nullItem();
	}

	private void handleBaseRequestEntity(BaseRequest message, String key, long timeStamp) {
		switch (message.getRequestType()) {
			case AppConstants.APPEND_REQUEST:
				logger.debug("Append got called: " + key);
				subscriptionService.checkSubscriptionsWithDelta(message, timeStamp,
						AppConstants.OPERATION_APPEND_ENTITY);
				break;
			case AppConstants.CREATE_REQUEST:
				logger.debug("Create got called: " + key);
				subscriptionService.checkSubscriptionsWithAbsolute(message, timeStamp,
						AppConstants.OPERATION_CREATE_ENTITY);
				break;
			case AppConstants.UPDATE_REQUEST:
				logger.debug("Update got called: " + key);
				subscriptionService.checkSubscriptionsWithDelta(message, timeStamp,
						AppConstants.OPERATION_UPDATE_ENTITY);
				break;
			case AppConstants.DELETE_REQUEST:
				logger.debug("Delete got called: " + key);
				subscriptionService.checkSubscriptionsWithAbsolute(message, timeStamp,
						AppConstants.OPERATION_DELETE_ENTITY);
				break;
			default:
				break;
		}
	}

	private void handleBaseRequestInternalNotification(InternalNotification message) {
		subscriptionService.handleRegistryNotification(message);
	}
}
