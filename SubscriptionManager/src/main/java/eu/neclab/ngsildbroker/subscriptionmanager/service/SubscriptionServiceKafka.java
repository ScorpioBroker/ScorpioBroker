package eu.neclab.ngsildbroker.subscriptionmanager.service;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.eclipse.microprofile.reactive.messaging.Channel;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.SyncMessage;
import io.quarkus.arc.profile.UnlessBuildProfile;
import io.smallrye.reactive.messaging.MutinyEmitter;

@Singleton
@UnlessBuildProfile("in-memory")
public class SubscriptionServiceKafka extends SubscriptionService{
	
	@Inject
	@Channel(AppConstants.SUB_SYNC_CHANNEL)
	MutinyEmitter<SyncMessage> syncEmitter;

	@Override
	protected MutinyEmitter<SyncMessage> getSyncChannelSender() {
		return syncEmitter;
	}


}
