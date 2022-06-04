package eu.neclab.ngsildbroker.historymanager.messaging;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import com.github.jsonldjava.utils.JsonUtils;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.AppendHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.HistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.UpdateHistoryEntityRequest;
import eu.neclab.ngsildbroker.historymanager.service.HistoryService;
import io.smallrye.mutiny.Uni;

public abstract class HistoryMessagingBase {
	private static Logger logger = LoggerFactory.getLogger(HistoryMessagingKafka.class);
	private ThreadPoolExecutor entityExecutor = new ThreadPoolExecutor(10, 50, 1, TimeUnit.MINUTES,
			new LinkedBlockingQueue<Runnable>());

	@Inject
	HistoryService historyService;

	public Uni<Void> baseHandleEntity(Message<BaseRequest> message) {
		try {
			logger.debug("received message in history from entity channel"
					+ JsonUtils.toString(message.getPayload().getFinalPayload()));
		} catch (Exception e) {
			logger.error("failed to output debug", e);
		}
		entityExecutor.execute(new Runnable() {

			@Override
			public void run() {
				HistoryEntityRequest request;
				try {
					switch (message.getPayload().getRequestType()) {
						case AppConstants.APPEND_REQUEST:
							logger.debug("Append got called: " + message.getPayload().getId());
							request = new AppendHistoryEntityRequest(message.getPayload());
							break;
						case AppConstants.CREATE_REQUEST:
							logger.debug("Create got called: " + message.getPayload().getId());
							request = new CreateHistoryEntityRequest(message.getPayload());
							break;
						case AppConstants.UPDATE_REQUEST:
							logger.debug("Update got called: " + message.getPayload().getId());
							request = new UpdateHistoryEntityRequest(message.getPayload());
							break;
						case AppConstants.DELETE_REQUEST:
							logger.debug("Delete got called: " + message.getPayload().getId());
							request = null;
							break;
						default:
							request = null;
							break;
					}
					if (request != null) {
						historyService.handleRequest(request).discardItems().await().indefinitely();
					}
				} catch (Exception e) {
					logger.error("Internal history recording failed", e.getMessage());
				}

			}
		});
		return Uni.createFrom().nullItem();
	}

}
