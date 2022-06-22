package eu.neclab.ngsildbroker.registryhandler.messaging;

import javax.inject.Singleton;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import io.quarkus.arc.profile.UnlessBuildProfile;
import io.smallrye.mutiny.Uni;

@Singleton
@UnlessBuildProfile("kafka")
public class CSourceMessagingInMemory extends CSourceMessagingBase {

	@Incoming(AppConstants.ENTITY_CHANNEL)
	@UnlessBuildProfile("kafka")
	public Uni<Void> handleEntity(Message<BaseRequest> mutinyMessage) {
		return baseHandleEntity(MicroServiceUtils.deepCopyMessage(mutinyMessage));

	}
}
