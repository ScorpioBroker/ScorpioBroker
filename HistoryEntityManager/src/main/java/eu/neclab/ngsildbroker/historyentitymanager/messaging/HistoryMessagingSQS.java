package eu.neclab.ngsildbroker.historyentitymanager.messaging;

import jakarta.inject.Singleton;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment.Strategy;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BatchRequest;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import io.quarkus.arc.profile.IfBuildProfile;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.common.annotation.RunOnVirtualThread;
import io.smallrye.mutiny.Uni;

@Singleton
@IfBuildProfile(anyOf = {"sqs"})
public class HistoryMessagingSQS extends HistoryMessagingBase {

	@ConfigProperty(name = "scorpio.messaging.duplicate", defaultValue = "false")
	boolean duplicate;

	@Incoming(AppConstants.ENTITY_RETRIEVE_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	public Uni<Void> handleEntity(BaseRequest message) {
		if (duplicate) {
			return baseHandleEntity(MicroServiceUtils.deepCopyRequestMessage(message));
		}
		return baseHandleEntity(message);
	}

	public Uni<Void> handleBatchEntity(BatchRequest message) {
		if (duplicate) {
			return baseHandleBatch(MicroServiceUtils.deepCopyRequestMessage(message));
		}
		return baseHandleBatch(message);
	}

	@Incoming(AppConstants.REGISTRY_RETRIEVE_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	public Uni<Void> handleCsource(BaseRequest busMessage) {
		if (duplicate) {
			return baseHandleCsource(MicroServiceUtils.deepCopyRequestMessage(busMessage));
		}
		return baseHandleCsource(busMessage);
	}

	@Scheduled(every = "5s")
	@RunOnVirtualThread
	Uni<Void> checkBuffer() {
		return super.checkBuffer();
	}
}
