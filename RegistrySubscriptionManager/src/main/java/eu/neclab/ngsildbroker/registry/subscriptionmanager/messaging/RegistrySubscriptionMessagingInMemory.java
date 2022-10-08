package eu.neclab.ngsildbroker.registry.subscriptionmanager.messaging;

import javax.inject.Singleton;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import io.quarkus.arc.profile.IfBuildProfile;

import io.smallrye.mutiny.Uni;

@Singleton
@IfBuildProfile("in-memory")
public class RegistrySubscriptionMessagingInMemory extends RegistrySubscriptionMessagingBase {

	@Incoming(AppConstants.REGISTRY_CHANNEL)
	public Uni<Void> handleCsource(BaseRequest busMessage) {
		return baseHandleCsource(MicroServiceUtils.deepCopyRequestMessage(busMessage), busMessage.getSendTimestamp());
	}

	@Incoming(AppConstants.INTERNAL_SUBS_CHANNEL)
	public Uni<Void> handleSubscription(SubscriptionRequest busMessage) {
		return baseHandleSubscription(MicroServiceUtils.deepCopySubscriptionMessage(busMessage));
	}
}
