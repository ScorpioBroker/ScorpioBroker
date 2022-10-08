package eu.neclab.ngsildbroker.registryhandler.messaging;

import javax.inject.Singleton;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import io.quarkus.arc.profile.UnlessBuildProfile;
import io.smallrye.mutiny.Uni;

@Singleton
@UnlessBuildProfile("in-memory")
public class CSourceMessagingKafka extends CSourceMessagingBase {
	@ConfigProperty(name = "scorpio.messaging.duplicate", defaultValue = "false")
	boolean duplicate;

	@Incoming(AppConstants.ENTITY_RETRIEVE_CHANNEL)
	public Uni<Void> handleEntity(BaseRequest mutinyMessage) {
		if (duplicate) {
			return baseHandleEntity(MicroServiceUtils.deepCopyRequestMessage(mutinyMessage));
		}
		return baseHandleEntity(mutinyMessage);

	}

}
