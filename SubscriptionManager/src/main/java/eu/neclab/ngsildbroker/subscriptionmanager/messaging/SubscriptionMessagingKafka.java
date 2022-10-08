package eu.neclab.ngsildbroker.subscriptionmanager.messaging;

import javax.inject.Singleton;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.InternalNotification;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import io.quarkus.arc.profile.UnlessBuildProfile;
import io.smallrye.mutiny.Uni;

@Singleton
@UnlessBuildProfile("in-memory")
public class SubscriptionMessagingKafka extends SubscriptionMessagingBase {

	@ConfigProperty(name = "scorpio.messaging.duplicate", defaultValue = "false")
	boolean duplicate;

	@Incoming(AppConstants.ENTITY_RETRIEVE_CHANNEL)
	public Uni<Void> handleEntity(BaseRequest message) {
		if (duplicate) {
			return baseHandleEntity(MicroServiceUtils.deepCopyRequestMessage(message), message.getSendTimestamp());
		}
		return baseHandleEntity(message, message.getSendTimestamp());
	}

	@Incoming(AppConstants.INTERNAL_RETRIEVE_NOTIFICATION_CHANNEL)
	public Uni<Void> handleInternalNotification(InternalNotification message) {
		return baseHandleInternalNotification(message);
	}
}
