package eu.neclab.ngsildbroker.historyentitymanager.messaging;

import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment.Strategy;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import io.quarkus.arc.profile.IfBuildProfile;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Singleton;

@Singleton
@IfBuildProfile(anyOf = { "mqtt", "rabbitmq" })
public class HistoryMessagingByteArray extends HistoryMessagingBase {

	
	
	
	@Incoming(AppConstants.REGISTRY_RETRIEVE_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	public Uni<Void> handleCsource(byte[] byteMessage) {
		return handleCsourceRaw(new String(byteMessage));
	}

	@Incoming(AppConstants.ENTITY_RETRIEVE_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	public Uni<Void> handleEntity(byte[] byteMessage) {
		return handleEntityRaw(new String(byteMessage));
	}


	

	@Scheduled(every = "5s", delayed = "${scorpio.startupdelay}")
	void checkBuffer() {
		super.checkBuffer();
	}

}
