package eu.neclab.ngsildbroker.subscriptionmanager.messaging;

import jakarta.enterprise.context.ApplicationScoped;
import io.quarkus.runtime.Startup;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment.Strategy;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import io.quarkus.arc.profile.IfBuildProfile;
import io.smallrye.mutiny.Uni;

@ApplicationScoped
@Startup
@IfBuildProfile(anyOf = { "mqtt"})
public class SubscriptionMessagingByteArray extends SubscriptionMessagingBase {

	@Incoming(AppConstants.ENTITY_RETRIEVE_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	public Uni<Void> handleEntity(byte[] byteMessage) {
		return handleEntityRaw(new String(byteMessage));
	}

	@Incoming(AppConstants.REGISTRY_RETRIEVE_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	public Uni<Void> handleCsource(byte[] byteMessage) {
		return handleCsourceRaw(new String(byteMessage));
	}

}
