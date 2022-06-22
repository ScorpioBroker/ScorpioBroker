package eu.neclab.ngsildbroker.subscriptionmanager.messaging;

import javax.inject.Singleton;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.InternalNotification;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import io.quarkus.arc.profile.UnlessBuildProfile;
import io.smallrye.mutiny.Uni;

@Singleton
@UnlessBuildProfile("kafka")
public class SubscriptionMessagingInMemory extends SubscriptionMessagingBase {

	@Incoming(AppConstants.ENTITY_CHANNEL)
	@UnlessBuildProfile("kafka")
	public Uni<Void> handleEntity(Message<BaseRequest> message) {
		return baseHandleEntity(MicroServiceUtils.deepCopyRequestMessage(message));
	}

	@Incoming(AppConstants.INTERNAL_NOTIFICATION_CHANNEL)
	@UnlessBuildProfile("kafka")
	public Uni<Void> handleInternalNotification(Message<InternalNotification> message) {
		return baseHandleInternalNotification(message);
	}
}
