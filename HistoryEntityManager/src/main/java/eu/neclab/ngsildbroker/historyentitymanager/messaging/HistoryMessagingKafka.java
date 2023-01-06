package eu.neclab.ngsildbroker.historyentitymanager.messaging;

import javax.inject.Singleton;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment.Strategy;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import io.quarkus.arc.profile.UnlessBuildProfile;
import io.smallrye.mutiny.Uni;

@Singleton
@UnlessBuildProfile("in-memory")
public class HistoryMessagingKafka extends HistoryMessagingBase {

	@ConfigProperty(name = "scorpio.messaging.duplicate", defaultValue = "false")
	boolean duplicate;

	@Incoming(AppConstants.ENTITY_RETRIEVE_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	public Uni<Void> handleEntity(BaseRequest message) {
		if (duplicate) {
			return baseHandleEntity(MicroServiceUtils.deepCopyRequestMessage(message));
		}
		return baseHandleEntity(message);
	}
}
