package eu.neclab.ngsildbroker.subscriptionmanager.service;

import java.util.UUID;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.interfaces.AnnouncementMessage;
import eu.neclab.ngsildbroker.commons.subscriptionbase.BaseSubscriptionSyncManager;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;

@Singleton
public class SubscriptionSyncService extends BaseSubscriptionSyncManager {

	public static final String SYNC_ID = UUID.randomUUID().toString();

	@Inject
	@Channel(AppConstants.SUB_SYNC_CHANNEL)
	MutinyEmitter<AnnouncementMessage> syncEmitter;

	@Incoming(AppConstants.SUB_SYNC_CHANNEL)
	private Uni<Void> listenForSubs(Message<SubscriptionRequest> message) {
		listenForSubscriptionUpdates(message.getPayload(), message.getPayload().getId());
		return Uni.createFrom().nullItem();
	}

	@Incoming(AppConstants.SUB_ALIVE_CHANNEL)
	private Uni<Void> listenForAlive(Message<AnnouncementMessage> message) {
		listenForAnnouncements(message.getPayload(), message.getPayload().getId());
		return Uni.createFrom().nullItem();
	}

	@Override
	protected void setSyncId() {
		this.syncId = SubscriptionSyncService.SYNC_ID;
	}

	@Override
	protected MutinyEmitter<AnnouncementMessage> getSyncEmitter() {
		return syncEmitter;
	}

}
