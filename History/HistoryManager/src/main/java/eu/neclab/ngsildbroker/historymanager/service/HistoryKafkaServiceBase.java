package eu.neclab.ngsildbroker.historymanager.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.AppendHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.HistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.UpdateHistoryEntityRequest;

public abstract class HistoryKafkaServiceBase {

	private static Logger logger = LoggerFactory.getLogger(HistoryKafkaServiceBase.class);

	@Autowired
	HistoryService historyService;

	public void handleBaseMessage(String key, BaseRequest message) {
		HistoryEntityRequest request;
		try {
			switch (message.getRequestType()) {
			case AppConstants.APPEND_REQUEST:
				logger.debug("Append got called: " + key);
				request = new AppendHistoryEntityRequest(message);
				break;
			case AppConstants.CREATE_REQUEST:
				logger.debug("Create got called: " + key);
				request = new CreateHistoryEntityRequest(message);
				break;
			case AppConstants.UPDATE_REQUEST:
				logger.debug("Update got called: " + key);
				request = new UpdateHistoryEntityRequest(message);
				break;
			case AppConstants.DELETE_REQUEST:
				logger.debug("Delete got called: " + key);
				request = null;
				break;
			default:
				request = null;
				break;
			}
			if (request != null) {
				historyService.handleRequest(request);
			}
		} catch (Exception e) {
			logger.error("Internal history recording failed", e.getMessage());
		}

	}
}
