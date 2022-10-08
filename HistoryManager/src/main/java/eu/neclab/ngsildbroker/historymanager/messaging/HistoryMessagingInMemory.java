package eu.neclab.ngsildbroker.historymanager.messaging;

import javax.inject.Singleton;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import io.quarkus.arc.profile.IfBuildProfile;
import io.smallrye.mutiny.Uni;

@Singleton
@IfBuildProfile("in-memory")
public class HistoryMessagingInMemory extends HistoryMessagingBase {

	@Incoming(AppConstants.ENTITY_CHANNEL)
	public Uni<Void> handleEntity(BaseRequest message) {
		// need to make a real copy of the message
		return baseHandleEntity(MicroServiceUtils.deepCopyRequestMessage(message));
	}
}
