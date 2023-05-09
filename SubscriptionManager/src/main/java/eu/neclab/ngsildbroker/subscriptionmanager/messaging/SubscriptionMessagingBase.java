package eu.neclab.ngsildbroker.subscriptionmanager.messaging;

import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BatchRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.InternalNotification;
import eu.neclab.ngsildbroker.subscriptionmanager.service.SubscriptionService;
import io.smallrye.mutiny.Uni;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.List;

public abstract class SubscriptionMessagingBase {
	private static final Logger logger = LoggerFactory.getLogger(SubscriptionMessagingBase.class);
	@Inject
	SubscriptionService subscriptionService;

	public Uni<Void> baseHandleEntity(BaseRequest message) {
		logger.debug("CSource sub manager got called for csource: " + message.getId());
		BatchRequest batchRequest = new BatchRequest(message.getTenant(), List.of(message.getPayload()), null,
				message.getRequestType());
		return subscriptionService.checkSubscriptions(batchRequest);
	}

	public Uni<Void> baseHandleBatchEntities(BatchRequest message) {
		logger.debug("CSource sub manager got called for csource: " + message.getEntityIds());
		return subscriptionService.checkSubscriptions(message);
	}

	public Uni<Void> baseHandleInternalNotification(InternalNotification message) {
		return subscriptionService.handleRegistryNotification(message);
	}

}
