package eu.neclab.ngsildbroker.historyentitymanager.messaging;

import javax.inject.Singleton;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment.Strategy;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BatchRequest;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import io.quarkus.arc.profile.IfBuildProfile;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Uni;

@Singleton
@IfBuildProfile("in-memory")
public class HistoryMessagingInMemory extends HistoryMessagingBase {

	@Incoming(AppConstants.ENTITY_CHANNEL)
	@Acknowledgment(Strategy.NONE)
	public Uni<Void> handleEntity(BaseRequest message) {
		// need to make a real copy of the message because in memory means in memory
		// reference so history will manipulate the subscriptions potentially
		return baseHandleEntity(MicroServiceUtils.deepCopyRequestMessage(message));
	}
	
	@Incoming(AppConstants.ENTITY_BATCH_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	public Uni<Void> handleBatchEntity(BatchRequest message) {
		// need to make a real copy of the message because in memory means in memory
		// reference so history will manipulate the subscriptions potentially
		return baseHandleBatch(MicroServiceUtils.deepCopyRequestMessage(message));
	}
	
	@Incoming(AppConstants.REGISTRY_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	public Uni<Void> handleCsource(BaseRequest busMessage) {
		return baseHandleCsource(MicroServiceUtils.deepCopyRequestMessage(busMessage));
	}
	
	@Scheduled(every = "5s")
	Uni<Void> checkBuffer() {
		return super.checkBuffer();
	}
}
