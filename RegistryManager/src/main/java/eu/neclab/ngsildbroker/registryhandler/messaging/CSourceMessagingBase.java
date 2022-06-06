package eu.neclab.ngsildbroker.registryhandler.messaging;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.github.jsonldjava.utils.JsonUtils;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.registryhandler.service.CSourceService;
import io.smallrye.mutiny.Uni;

public abstract class CSourceMessagingBase {

	@Inject
	CSourceService cSourceService;

	private static final Logger logger = LoggerFactory.getLogger(CSourceMessagingKafka.class);

	private ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 50, 1, TimeUnit.MINUTES,
			new LinkedBlockingQueue<Runnable>());

	public Uni<Void> baseHandleEntity(Message<BaseRequest> mutinyMessage) {

		try {
			logger.debug("received message in csource from entity channel"
					+ JsonUtils.toString(mutinyMessage.getPayload().getFinalPayload()));
		} catch (Exception e) {
			logger.error("failed to output debug", e);
		}
		BaseRequest message = mutinyMessage.getPayload();
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
						cSourceService.handleEntityCreateOrUpdate(message);
						break;
					default:
						break;
				}
			};
		});
		return Uni.createFrom().nullItem();
	}

}
