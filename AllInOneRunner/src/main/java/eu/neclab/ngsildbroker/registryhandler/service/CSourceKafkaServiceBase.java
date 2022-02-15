package eu.neclab.ngsildbroker.registryhandler.service;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;

public abstract class CSourceKafkaServiceBase {
	private static final Logger logger = LoggerFactory.getLogger(CSourceKafkaServiceBase.class);

	@Inject
	CSourceService cSourceService;

	public void handleBaseRequest(BaseRequest message, String key) {
		logger.debug("received entity request " + message.toString());
		switch (message.getRequestType()) {
		case AppConstants.DELETE_REQUEST:
			cSourceService.handleEntityDelete(message);
			break;
		case AppConstants.UPDATE_REQUEST:
		case AppConstants.CREATE_REQUEST:
		case AppConstants.DELETE_ATTRIBUTE_REQUEST:
		case AppConstants.APPEND_REQUEST:
			cSourceService.handleEntityCreateOrUpdate(message);
			break;
		default:
			break;
		}
	}

}
