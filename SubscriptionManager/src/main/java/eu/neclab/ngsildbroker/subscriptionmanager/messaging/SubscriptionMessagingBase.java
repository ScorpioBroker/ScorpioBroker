package eu.neclab.ngsildbroker.subscriptionmanager.messaging;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import com.github.jsonldjava.utils.JsonUtils;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.InternalNotification;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.subscriptionmanager.service.SubscriptionService;
import io.smallrye.mutiny.Uni;


public abstract class SubscriptionMessagingBase {
	private static final Logger logger = LoggerFactory.getLogger(SubscriptionMessagingBase.class);
	private ThreadPoolExecutor entityExecutor = new ThreadPoolExecutor(10, 50, 1, TimeUnit.MINUTES,
			new LinkedBlockingQueue<Runnable>());
	private ThreadPoolExecutor notificationExecutor = new ThreadPoolExecutor(10, 50, 1, TimeUnit.MINUTES,
			new LinkedBlockingQueue<Runnable>());

	@Inject
	SubscriptionService subscriptionService;

	public Uni<Void> baseHandleEntity(Message<BaseRequest> message, long timestamp) {
		

		BaseRequest payload = new BaseRequest(message.getPayload());
		entityExecutor.execute(new Runnable() {
			@Override
			public void run() {
				handleBaseRequestEntity(payload, payload.getId(), timestamp);
			}
		});
		return Uni.createFrom().nullItem();
	}

	public Uni<Void> baseHandleInternalNotification(Message<InternalNotification> message) {
		notificationExecutor.execute(new Runnable() {
			@Override
			public void run() {
				handleBaseRequestInternalNotification(message.getPayload());
			}
		});
		return Uni.createFrom().nullItem();
	}

	private void handleBaseRequestEntity(BaseRequest message, String key, long timeStamp) {
		try {
			logger.debug(JsonUtils.toPrettyString(message.getFinalPayload()));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
