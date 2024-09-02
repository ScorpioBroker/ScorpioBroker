package eu.neclab.ngsildbroker.registry.subscriptionmanager.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.SyncMessage;
import eu.neclab.ngsildbroker.registry.subscriptionmanager.service.RegistrySubscriptionService;
import io.netty.channel.EventLoopGroup;
import io.smallrye.mutiny.Uni;

public abstract class RegistrySubscriptionSyncServiceBase implements SyncService {

	protected static final Logger logger = LoggerFactory.getLogger(RegistrySubscriptionSyncServiceBase.class);

	protected Uni<Void> baseHandleSync(SyncMessage message, String syncId, RegistrySubscriptionService subService,
			EventLoopGroup executor) {
		String key = message.getSyncId();

		if (key.equals(syncId) || message.getSubType() == SyncMessage.NORMAL_SUB) {
			return Uni.createFrom().voidItem();
		}
		switch (message.getRequestType()) {
		case AppConstants.DELETE_SUBSCRIPTION_REQUEST:
			subService.syncDeleteSubscription(message.getTenant(), message.getSubId()).runSubscriptionOn(executor)
					.subscribe().with(v -> logger.debug("done handling delete"));
			break;
		case AppConstants.UPDATE_SUBSCRIPTION_REQUEST:
			subService.syncUpdateSubscription(message.getTenant(), message.getSubId()).runSubscriptionOn(executor).subscribe()
					.with(v -> logger.debug("done handling update"));
			break;
		case AppConstants.CREATE_SUBSCRIPTION_REQUEST:
			subService.syncUpdateSubscription(message.getTenant(), message.getSubId()).runSubscriptionOn(executor).subscribe()
					.with(v -> logger.debug("done handling create"));
			break;
		default:
			logger.debug("default");
			break;
		}

		return Uni.createFrom().voidItem();
	}
}
