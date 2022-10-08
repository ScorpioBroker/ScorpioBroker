package eu.neclab.ngsildbroker.subscriptionmanager.messaging;

import javax.inject.Singleton;

import org.eclipse.microprofile.reactive.messaging.Incoming;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.InternalNotification;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import io.quarkus.arc.profile.IfBuildProfile;
import io.smallrye.mutiny.Uni;

@Singleton
@IfBuildProfile("in-memory")
public class SubscriptionMessagingInMemory extends SubscriptionMessagingBase {

	@Incoming(AppConstants.ENTITY_CHANNEL)
	public Uni<Void> handleEntity(BaseRequest message) {
		return baseHandleEntity(MicroServiceUtils.deepCopyRequestMessage(message), message.getSendTimestamp());
	}

	@Incoming(AppConstants.INTERNAL_NOTIFICATION_CHANNEL)
	public Uni<Void> handleInternalNotification(InternalNotification message) {
		return baseHandleInternalNotification(message);
	}
}
