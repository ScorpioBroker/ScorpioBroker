package eu.neclab.ngsildbroker.registryhandler.messaging;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.registryhandler.service.CSourceService;
import io.smallrye.mutiny.Uni;

public abstract class CSourceMessagingBase {

	@Inject
	CSourceService cSourceService;
	@ConfigProperty(name = "scorpio.registry.autorecording", defaultValue = "active")
	String AUTO_REG_STATUS;

	private static final Logger logger = LoggerFactory.getLogger(CSourceMessagingKafka.class);

	private ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 50, 1, TimeUnit.MINUTES,
			new LinkedBlockingQueue<Runnable>());

	public Uni<Void> baseHandleEntity(BaseRequest mutinyMessage) {
		BaseRequest message = mutinyMessage;
		executor.execute(new Runnable() {

			@Override
			public void run() {
				switch (message.getRequestType()) {
					case AppConstants.DELETE_REQUEST:
						cSourceService.handleEntityDelete(message);
						break;
					case AppConstants.UPDATE_REQUEST:
					case AppConstants.CREATE_REQUEST:
					case AppConstants.DELETE_ATTRIBUTE_REQUEST:
					case AppConstants.APPEND_REQUEST:
						if (AUTO_REG_STATUS.equals("active")) {
							cSourceService.handleEntityCreateOrUpdate(message);
						}
						break;
					default:
						break;
				}
			};
		});
		return Uni.createFrom().voidItem();
	}

}
