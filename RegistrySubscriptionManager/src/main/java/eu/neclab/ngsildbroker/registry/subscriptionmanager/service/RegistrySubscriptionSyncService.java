package eu.neclab.ngsildbroker.registry.subscriptionmanager.service;

import java.util.UUID;

import javax.inject.Singleton;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.interfaces.AnnouncementMessage;
import eu.neclab.ngsildbroker.commons.subscriptionbase.BaseSubscriptionSyncManager;

@Singleton
public class RegistrySubscriptionSyncService extends BaseSubscriptionSyncManager {

	public static final String SYNC_ID = UUID.randomUUID().toString();

	@Incoming(AppConstants.REG_SUB_TOPICS_CHANNEL)
	private void listenForSubs(Message<SubscriptionRequest> message) {
		listenForSubscriptionUpdates(message.getPayload(), message.getPayload().getId());
	}

	@Incoming(AppConstants.REG_SUB_ALIVE_CHANNEL)
	private void listenForAlive(Message<AnnouncementMessage> message) {
		listenForAnnouncements(message.getPayload(), message.getPayload().getId());
	}

	@Override
	protected void setSyncId() {
		this.syncId = RegistrySubscriptionSyncService.SYNC_ID;
	}

}
