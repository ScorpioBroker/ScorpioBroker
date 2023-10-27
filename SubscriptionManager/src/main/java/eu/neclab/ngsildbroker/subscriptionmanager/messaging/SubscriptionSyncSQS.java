package eu.neclab.ngsildbroker.subscriptionmanager.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.SubscriptionRequest;
import eu.neclab.ngsildbroker.subscriptionmanager.service.SubscriptionService;
import io.quarkus.arc.profile.IfBuildProfile;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.pgclient.PgPool;
import io.vertx.mutiny.pgclient.pubsub.PgSubscriber;
import io.vertx.mutiny.sqlclient.Tuple;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@IfBuildProfile("sqs")
@Singleton
public class SubscriptionSyncSQS implements SyncService {

	@Inject
	PgSubscriber pgSubscriber;

	@Inject
	PgPool pgPool;

	@Inject
	SubscriptionService subService;

	Logger logger = LoggerFactory.getLogger(SubscriptionSyncSQS.class);

	private String seperator = "<&>";

	@PostConstruct
	void setup() {
		subService.addSyncService(this);
		pgSubscriber.channel("subscription-channel").handler(notice -> {
			logger.info("notice received: " + notice);
			String[] splitted = notice.split(seperator);
			int requestType = Integer.parseInt(splitted[2]);
			subService.reloadSubscription(splitted[1], splitted[0]);
		});
		pgSubscriber.connect().await().indefinitely();
	}

	@Override
	public Uni<Void> sync(SubscriptionRequest request) {
		logger.info("sending notify: ");
		return pgPool.preparedQuery("NOTIFY \"subscription-channel\" $1")
				.execute(Tuple
						.of(request.getId() + seperator + request.getTenant() + seperator + request.getRequestType()))
				.onItem().transformToUni(r -> Uni.createFrom().voidItem());

	}

}
