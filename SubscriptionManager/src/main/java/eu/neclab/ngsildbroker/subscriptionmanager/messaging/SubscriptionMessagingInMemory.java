package eu.neclab.ngsildbroker.subscriptionmanager.messaging;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BatchRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.InternalNotification;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import io.quarkus.arc.profile.IfBuildProfile;
import io.smallrye.mutiny.Uni;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment.Strategy;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import javax.inject.Singleton;

@Singleton
@IfBuildProfile("in-memory")
public class SubscriptionMessagingInMemory extends SubscriptionMessagingBase {

	@Incoming(AppConstants.ENTITY_CHANNEL)
	@Acknowledgment(Strategy.NONE)
	public Uni<Void> handleEntity(BaseRequest message) {
		return baseHandleEntity(MicroServiceUtils.deepCopyRequestMessage(message));
	}

	@Incoming(AppConstants.ENTITY_BATCH_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	public Uni<Void> handleBatchEntities(BatchRequest message) {
		return baseHandleBatchEntities(MicroServiceUtils.deepCopyRequestMessage(message));
	}

	@Incoming(AppConstants.INTERNAL_NOTIFICATION_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	public Uni<Void> handleInternalNotification(InternalNotification message) {
		return baseHandleInternalNotification(message);
	}
}
