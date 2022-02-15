package eu.neclab.ngsildbroker.registryhandler.service;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.inject.Singleton;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import io.smallrye.mutiny.Uni;

@Singleton
public class CSourceKafkaService extends CSourceKafkaServiceBase {

	private ThreadPoolExecutor kafkaExecutor = new ThreadPoolExecutor(10, 50, 1, TimeUnit.MINUTES,
			new LinkedBlockingQueue<Runnable>());
	@Incoming(AppConstants.ENTITY_RETRIEVE_CHANNEL)
	public Uni<Void> handleEntity(Message<BaseRequest> message) {
		kafkaExecutor.execute(new Runnable() {
			@Override
			public void run() {
				handleBaseRequest(message.getPayload(), message.getPayload().getId());
			}
		});
		return Uni.createFrom().nullItem();
	}

}
