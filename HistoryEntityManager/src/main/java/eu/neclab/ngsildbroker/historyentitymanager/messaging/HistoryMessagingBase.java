package eu.neclab.ngsildbroker.historyentitymanager.messaging;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BatchRequest;
import eu.neclab.ngsildbroker.historyentitymanager.service.HistoryEntityService;
//import eu.neclab.ngsildbroker.historyentitymanager.service.HistoryEntityService;
import io.smallrye.mutiny.Uni;


public abstract class HistoryMessagingBase {

	private static Logger logger = LoggerFactory.getLogger(HistoryMessagingBase.class);
	
	
	@Inject
	HistoryEntityService historyService;

	public Uni<Void> baseHandleEntity(BaseRequest message) {
		logger.debug("history manager got called for entity: " + message.getId());
		return historyService.handleInternalRequest(message);
	}

	public Uni<Void> baseHandleBatch(BatchRequest message) {
		logger.debug("history manager batch handling got called");
		return historyService.handleInternalBatchRequest(message);
	}
	
	public Uni<Void> baseHandleCsource(BaseRequest message) {
		logger.debug("history manager got called for csource: " + message.getId());
		return historyService.handleRegistryChange(message);
	}

}
