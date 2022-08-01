package eu.neclab.ngsildbroker.registry.subscriptionmanager.messaging;

import java.util.UUID;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.AliveAnnouncement;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.subscriptionbase.BaseSubscriptionService;
import eu.neclab.ngsildbroker.commons.subscriptionbase.BaseSubscriptionSyncManager;
import eu.neclab.ngsildbroker.registry.subscriptionmanager.service.RegistrySubscriptionService;
import io.quarkus.arc.profile.UnlessBuildProfile;
import io.smallrye.mutiny.Uni;

@Singleton
@UnlessBuildProfile("in-memory")
public class RegistrySubscriptionSyncService extends BaseSubscriptionSyncManager {

	public static final String SYNC_ID = UUID.randomUUID().toString();

	@Inject
	@Channel(AppConstants.REG_SUB_ALIVE_CHANNEL)
	Emitter<AliveAnnouncement> aliveEmitter;

	@Inject
	RegistrySubscriptionService subService;

	@Incoming(AppConstants.REG_SUB_SYNC_RETRIEVE_CHANNEL)
	Uni<Void> listenForSubs(Message<SubscriptionRequest> message) {
		listenForSubscriptionUpdates(message.getPayload(), message.getPayload().getId());
		return Uni.createFrom().nullItem();
	}

	@Incoming(AppConstants.REG_SUB_ALIVE_RETRIEVE_CHANNEL)
	Uni<Void> listenForAlive(Message<AliveAnnouncement> message) {
		listenForAnnouncements(message.getPayload(), message.getPayload().getId());
		return Uni.createFrom().nullItem();
	}

	@Override
	protected void setSyncId() {
		this.syncId = RegistrySubscriptionSyncService.SYNC_ID;
	}

	@Override
	protected Emitter<AliveAnnouncement> getAliveEmitter() {
		return aliveEmitter;

	}

	@Override
	protected BaseSubscriptionService getSubscriptionService() {
		return subService;
	}

}
