package eu.neclab.ngsildbroker.queryhandler.messaging;

import jakarta.inject.Singleton;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment.Strategy;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import io.quarkus.arc.profile.IfBuildProfile;
import io.smallrye.mutiny.Uni;

@Singleton
@IfBuildProfile("in-memory")
public class QueryManagerMessagingInMemory extends QueryManagerMessagingBase {
	
	@Incoming(AppConstants.REGISTRY_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	public Uni<Void> handleCsource(BaseRequest busMessage) {
		return baseHandleCsource(MicroServiceUtils.deepCopyRequestMessage(busMessage));
	}
}
