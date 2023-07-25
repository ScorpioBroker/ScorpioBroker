package eu.neclab.ngsildbroker.historyquerymanager.messaging;

import jakarta.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.historyquerymanager.service.HistoryQueryService;

import io.smallrye.mutiny.Uni;


public abstract class HistoryMessagingBase {

	private static Logger logger = LoggerFactory.getLogger(HistoryMessagingBase.class);
	
	
	@Inject
	HistoryQueryService historyService;


	public Uni<Void> baseHandleCsource(BaseRequest message) {
		logger.debug("history query manager got called for csource: " + message.getId());
		return historyService.handleRegistryChange(message);
	}

}
