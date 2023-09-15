package eu.neclab.ngsildbroker.historyentitymanager.messaging;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment.Strategy;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import io.quarkus.arc.profile.IfBuildProfile;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Uni;
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

	@Scheduled(every = "20s", delayed = "5s")
	void purge() {
		super.purge();
	}

}
