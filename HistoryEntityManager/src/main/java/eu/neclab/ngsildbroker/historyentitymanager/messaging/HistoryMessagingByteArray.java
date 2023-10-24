package eu.neclab.ngsildbroker.historyentitymanager.messaging;

import java.util.Map;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment.Strategy;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import io.quarkus.arc.profile.IfBuildProfile;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.common.annotation.RunOnVirtualThread;
import io.smallrye.mutiny.Uni;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;

@Singleton
@IfBuildProfile(anyOf = { "mqtt", "rabbitmq" })
public class HistoryMessagingByteArray extends HistoryMessagingBase {

	
	
	
	@Incoming(AppConstants.REGISTRY_RETRIEVE_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	public Uni<Void> handleCsource(byte[] byteMessage) {
		return handleCsourceRaw(new String(byteMessage));
	}

	@Incoming(AppConstants.ENTITY_RETRIEVE_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	public Uni<Void> handleEntity(byte[] byteMessage) {
		return handleEntityRaw(new String(byteMessage));
	}

	@Incoming(AppConstants.ENTITY_BATCH_RETRIEVE_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	public Uni<Void> handleBatchEntities(byte[] byteMessage) {
		return handleBatchEntitiesRaw(new String(byteMessage));
	}

	@Scheduled(every = "20s", delayed = "${scorpio.startupdelay}")
	void purge() {
		super.purge();
	}

	@Scheduled(every = "5s", delayed = "${scorpio.startupdelay}")
	@RunOnVirtualThread
	Uni<Void> checkBuffer() {
		return super.checkBuffer();
	}

	@Incoming(AppConstants.HIST_SYNC_RETRIEVE_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	public Uni<Void> handleAnnouncement(byte[] byteMessage) {
		return handleAnnouncement(new String(byteMessage));
	}
	
	@Scheduled(every = "${scorpio.sync.check-time}", delayed = "${scorpio.startupdelay}")
	@RunOnVirtualThread
	void checkInstances() {
		super.checkInstances();
	}
	
	@Scheduled(every = "${scorpio.sync.announcement-time}", delayed = "${scorpio.startupdelay}")
	void syncTask() {
		MicroServiceUtils.serializeAndSplitObjectAndEmit(announcement, Integer.MAX_VALUE, syncEmitter, objectMapper);
	}

	@PreDestroy
	void shutdown() {
		MicroServiceUtils.serializeAndSplitObjectAndEmit(Map.of("instanceId", myInstanceId, "upOrDown", false), Integer.MAX_VALUE, syncEmitter, objectMapper);
	}

	

}
