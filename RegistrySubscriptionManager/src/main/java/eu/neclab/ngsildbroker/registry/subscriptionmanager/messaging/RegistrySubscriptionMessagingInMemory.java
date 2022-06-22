package eu.neclab.ngsildbroker.registry.subscriptionmanager.messaging;

import javax.inject.Singleton;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import io.quarkus.arc.profile.UnlessBuildProfile;
import io.smallrye.mutiny.Uni;

@Singleton
@UnlessBuildProfile("kafka")
public class RegistrySubscriptionMessagingInMemory extends RegistrySubscriptionMessagingBase {

	@Incoming(AppConstants.REGISTRY_CHANNEL)
	@UnlessBuildProfile("kafka")
	public Uni<Void> handleCsource(Message<BaseRequest> busMessage) {
		return baseHandleCsource(MicroServiceUtils.deepCopyMessage(busMessage));
	}

	@Incoming(AppConstants.INTERNAL_SUBS_CHANNEL)
	@UnlessBuildProfile("kafka")
	public Uni<Void> handleSubscription(Message<SubscriptionRequest> busMessage) {
		return baseHandleSubscription(busMessage);
	}
}
