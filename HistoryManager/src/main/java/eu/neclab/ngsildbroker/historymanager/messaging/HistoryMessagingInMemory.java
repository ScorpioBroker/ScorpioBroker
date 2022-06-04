package eu.neclab.ngsildbroker.historymanager.messaging;

import javax.inject.Singleton;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import io.quarkus.arc.profile.UnlessBuildProfile;
import io.smallrye.mutiny.Uni;

@Singleton
@UnlessBuildProfile("kafka")
public class HistoryMessagingInMemory extends HistoryMessagingBase {

	@Incoming(AppConstants.ENTITY_CHANNEL)
	@UnlessBuildProfile("kafka")
	public Uni<Void> handleEntity(Message<BaseRequest> message) {
		return baseHandleEntity(message);
	}
}
