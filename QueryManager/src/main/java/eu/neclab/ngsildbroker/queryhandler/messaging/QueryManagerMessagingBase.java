package eu.neclab.ngsildbroker.queryhandler.messaging;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;

import eu.neclab.ngsildbroker.queryhandler.services.QueryService;
import io.smallrye.mutiny.Uni;


public abstract class QueryManagerMessagingBase {

	private static Logger logger = LoggerFactory.getLogger(QueryManagerMessagingBase.class);
	
	
	@Inject
	QueryService queryService;


	public Uni<Void> baseHandleCsource(BaseRequest message) {
		logger.debug("query manager got called for csource: " + message.getId());
		return queryService.handleRegistryChange(message);
	}

}
