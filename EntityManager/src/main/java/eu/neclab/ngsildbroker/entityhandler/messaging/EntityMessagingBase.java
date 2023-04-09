package eu.neclab.ngsildbroker.entityhandler.messaging;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.entityhandler.services.EntityService;
import io.smallrye.mutiny.Uni;

public abstract class EntityMessagingBase {

	private final static Logger logger = LoggerFactory.getLogger(EntityMessagingBase.class);

	@Inject
	EntityService entityService;

	public Uni<Void> baseHandleCsource(BaseRequest message) {
		logger.debug("entity manager got called for csource: " + message.getId());
		return entityService.handleRegistryChange(message);
	}

}
