package eu.neclab.ngsildbroker.registry.subscriptionmanager.service;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.inject.Singleton;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import io.quarkus.arc.properties.IfBuildProperty;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;

@Singleton
@IfBuildProperty(name = "scorpio.kafka.enabled", enableIfMissing = true, stringValue = "true")
public class RegistrySubscriptionKafkaService extends RegistrySubscriptionKafkaServiceBase {

	private ThreadPoolExecutor registryExecutor = new ThreadPoolExecutor(10, 50, 1, TimeUnit.MINUTES,
			new LinkedBlockingQueue<Runnable>());
	private ThreadPoolExecutor subscriptionExecutor = new ThreadPoolExecutor(10, 50, 1, TimeUnit.MINUTES,
			new LinkedBlockingQueue<Runnable>());

	@Incoming(AppConstants.REGISTRY_RETRIEVE_CHANNEL)
	public Uni<Void> handleCsource(Message<BaseRequest> message) {
		registryExecutor.execute(new Runnable() {
			@Override
			public void run() {
				IncomingKafkaRecordMetadata metaData = message.getMetadata(IncomingKafkaRecordMetadata.class)
						.orElse(null);
				long timestamp = System.currentTimeMillis();
				if (metaData != null) {
					timestamp = metaData.getTimestamp().toEpochMilli();
				}
				BaseRequest payload = message.getPayload();
				handleBaseRequestRegistry(payload, payload.getId(), timestamp);
			}
		});
		return Uni.createFrom().nullItem();
	}

	@Incoming(AppConstants.INTERNAL_RETRIEVE_SUBS_CHANNEL)
	public Uni<Void> handleSubscription(Message<SubscriptionRequest> message) {
		subscriptionExecutor.execute(new Runnable() {
			@Override
			public void run() {
				handleBaseRequestSubscription(message.getPayload(), message.getPayload().getId());
			}
		});
		return Uni.createFrom().nullItem();
	}
}
