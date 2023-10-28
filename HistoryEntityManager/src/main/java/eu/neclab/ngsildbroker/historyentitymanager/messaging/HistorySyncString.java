package eu.neclab.ngsildbroker.historyentitymanager.messaging;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment.Strategy;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import io.quarkus.arc.profile.IfBuildProfile;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.common.annotation.RunOnVirtualThread;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Singleton;

@Singleton
@IfBuildProfile(anyOf = { "kafka" })
public class HistorySyncString extends HistorySync {

	@Incoming(AppConstants.HIST_SYNC_RETRIEVE_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	public Uni<Void> handleAnnouncement(String byteMessage) {
		return super.handleAnnouncement(byteMessage);
	}

	@Scheduled(every = "${scorpio.sync.check-time}", delayed = "${scorpio.startupdelay}")
	@RunOnVirtualThread
	void checkInstances() {
		super.checkInstances();
	}

	@Scheduled(every = "${scorpio.sync.announcement-time}", delayed = "${scorpio.startupdelay}")
	void syncTask() {
		super.syncTask();
	}

}
