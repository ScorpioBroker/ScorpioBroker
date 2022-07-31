package eu.neclab.ngsildbroker.subscriptionmanager.messaging;

import java.util.UUID;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.interfaces.AnnouncementMessage;
import eu.neclab.ngsildbroker.commons.subscriptionbase.BaseSubscriptionService;
import eu.neclab.ngsildbroker.commons.subscriptionbase.BaseSubscriptionSyncManager;
import eu.neclab.ngsildbroker.subscriptionmanager.service.SubscriptionService;
import io.quarkus.arc.profile.UnlessBuildProfile;

@Singleton
@UnlessBuildProfile("in-memory")
public class SubscriptionSyncService extends BaseSubscriptionSyncManager {

	public static final String SYNC_ID = UUID.randomUUID().toString();

	@Inject
	@Channel(AppConstants.SUB_ALIVE_CHANNEL)
	Emitter<AnnouncementMessage> aliveEmitter;

	@Inject
	SubscriptionService subService;

	@Incoming(AppConstants.SUB_SYNC_RETRIEVE_CHANNEL)
	void listenForSubs(Message<SubscriptionRequest> message) {
		listenForSubscriptionUpdates(message.getPayload(), message.getPayload().getId());
	}

	@Incoming(AppConstants.SUB_ALIVE_RETRIEVE_CHANNEL)
	void listenForAlive(Message<AnnouncementMessage> message) {
		listenForAnnouncements(message.getPayload(), message.getPayload().getId());
	}

	@Override
	protected void setSyncId() {
		this.syncId = SYNC_ID;
	}

	@Override
	protected Emitter<AnnouncementMessage> getAliveEmitter() {
		return aliveEmitter;
	}

	@Override
	protected BaseSubscriptionService getSubscriptionService() {
		return subService;
	}

}
