package eu.neclab.ngsildbroker.historyentitymanager.messaging;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.historyentitymanager.service.HistoryEntityService;
import io.smallrye.mutiny.Uni;

public abstract class HistoryMessagingBase {
	private ThreadPoolExecutor entityExecutor = new ThreadPoolExecutor(10, 50, 1, TimeUnit.MINUTES,
			new LinkedBlockingQueue<Runnable>());

	@Inject
	HistoryEntityService historyService;

	public Uni<Void> baseHandleEntity(BaseRequest message) {
		entityExecutor.execute(new Runnable() {
			@Override
			public void run() {
				historyService.handleInternalRequest(message);
			}
		});
		return Uni.createFrom().voidItem();
	}

}
