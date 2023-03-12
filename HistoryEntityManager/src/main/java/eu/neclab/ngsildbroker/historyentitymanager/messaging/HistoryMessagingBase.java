package eu.neclab.ngsildbroker.historyentitymanager.messaging;

import javax.inject.Inject;

import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
//import eu.neclab.ngsildbroker.historyentitymanager.service.HistoryEntityService;
import io.smallrye.mutiny.Uni;

public abstract class HistoryMessagingBase {

	// @Inject
	// HistoryEntityService historyService;

	public Uni<Void> baseHandleEntity(BaseRequest message) {
		// return historyService.handleInternalRequest(message);
		return Uni.createFrom().voidItem();

	}

}
