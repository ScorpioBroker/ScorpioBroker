package eu.neclab.ngsildbroker.registryhandler.messaging;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.registryhandler.service.CSourceService;
import io.smallrye.mutiny.Uni;

public abstract class CSourceMessagingBase {

	@Inject
	CSourceService cSourceService;

	private static final Logger logger = LoggerFactory.getLogger(CSourceMessagingKafka.class);

	private ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 50, 1, TimeUnit.MINUTES,
			new LinkedBlockingQueue<Runnable>());

	public Uni<Void> baseHandleEntity(BaseRequest mutinyMessage) {
		BaseRequest message = mutinyMessage;
		logger.debug("registry handler retrieved message " + message.getId());
		executor.execute(new Runnable() {

			@Override
			public void run() {
				cSourceService.handleEntityOperation(message);
		
			};
		});
		return Uni.createFrom().voidItem();
	}

}
