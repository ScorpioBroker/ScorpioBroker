package eu.neclab.ngsildbroker.registryhandler.messaging;

import javax.inject.Singleton;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import io.quarkus.arc.profile.IfBuildProfile;
import io.smallrye.mutiny.Uni;

@Singleton
@IfBuildProfile("in-memory")
public class CSourceMessagingInMemory extends CSourceMessagingBase {

	@Incoming(AppConstants.ENTITY_CHANNEL)
	public Uni<Void> handleEntity(BaseRequest mutinyMessage) {
		return baseHandleEntity(MicroServiceUtils.deepCopyRequestMessage(mutinyMessage));

	}
}
