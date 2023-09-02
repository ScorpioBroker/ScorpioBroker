package eu.neclab.ngsildbroker.historyentitymanager.messaging;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment.Strategy;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import io.quarkus.arc.profile.UnlessBuildProfile;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Singleton;

@Singleton
@UnlessBuildProfile("in-memory")
public class HistoryMessaging extends HistoryMessagingBase {

	@Incoming(AppConstants.REGISTRY_RETRIEVE_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	public Uni<Void> handleCsource(Object byteMessage) {
		if(byteMessage instanceof byte[] bytes) {
			byteMessage = new String(bytes);
		}
		return handleCsourceRaw((String) byteMessage);
	}

	@Incoming(AppConstants.ENTITY_RETRIEVE_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	public Uni<Void> handleEntity(Object byteMessage) {
		if(byteMessage instanceof byte[] bytes) {
			byteMessage = new String(bytes);
		}
		return handleEntityRaw((String) byteMessage);
	}

	@Incoming(AppConstants.ENTITY_BATCH_RETRIEVE_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	public Uni<Void> handleBatchEntities(Object byteMessage) {
		if(byteMessage instanceof byte[] bytes) {
			byteMessage = new String(bytes);
		}
		return handleBatchEntitiesRaw((String) byteMessage);
	}

}
