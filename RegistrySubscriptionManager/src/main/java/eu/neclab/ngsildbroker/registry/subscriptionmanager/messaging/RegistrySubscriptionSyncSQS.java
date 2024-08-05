package eu.neclab.ngsildbroker.registry.subscriptionmanager.messaging;

import java.util.UUID;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.storage.ClientManager;
import eu.neclab.ngsildbroker.registry.subscriptionmanager.service.RegistrySubscriptionService;
import io.quarkus.arc.profile.IfBuildProfile;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.pgclient.pubsub.PgSubscriber;
import io.vertx.pgclient.PgConnectOptions;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@IfBuildProfile("sqs")
@Singleton
public class RegistrySubscriptionSyncSQS implements SyncService {

	private final String SYNC_ID = UUID.randomUUID().toString();

	PgSubscriber pgSubscriber;

	@Inject
	ClientManager clientManager;

	@Inject
	Vertx vertx;

	@Inject
	RegistrySubscriptionService subService;

	@ConfigProperty(name = "quarkus.datasource.reactive.url")
	String reactiveDefaultUrl;
	@ConfigProperty(name = "quarkus.datasource.username")
	String username;
	@ConfigProperty(name = "quarkus.datasource.password")
	String password;

	Logger logger = LoggerFactory.getLogger(RegistrySubscriptionSyncSQS.class);

	private String seperator = "<&>";

	// This is needed so that @postconstruct runs on the startup thread and not on a
	// worker thread later on
	void startup(@Observes StartupEvent event) {
	}

	@PostConstruct
	void setup() {
		String tmp = reactiveDefaultUrl.substring("postgresql://".length());
		String[] splitted = tmp.split(":");
		String host = splitted[0];
		String[] tmp1 = splitted[1].split("/");
		String port = tmp1[0];
		String dbName = tmp1[1].split("\\?")[0];

		pgSubscriber = PgSubscriber.subscriber(vertx, new PgConnectOptions().setHost(host)
				.setPort(Integer.parseInt(port)).setDatabase(dbName).setUser(username).setPassword(password));
		subService.addSyncService(this);
		pgSubscriber.channel("regsubscriptionchannel").handler(notice -> {
			logger.debug("notice received: " + notice);
			String[] noticeSplitted = notice.split(seperator);
			int requestType = Integer.parseInt(noticeSplitted[2]);
			String syncId = noticeSplitted[3];
			boolean internal = noticeSplitted[3].equals("1");
			if (syncId.equals(SYNC_ID)) {
				logger.debug("Discarding own announcement");
			} else {
				subService.reloadSubscription(noticeSplitted[1], noticeSplitted[0], internal);
			}
		});
		pgSubscriber.connect().await().indefinitely();
	}

	@Override
	public Uni<Void> sync(SubscriptionRequest request) {
		logger.debug("sending notify: ");
		return clientManager.getClient(AppConstants.INTERNAL_NULL_KEY, false).onItem().transformToUni(client -> {
			String internal = "0";
			if (request.getSubscription().getNotification().getEndPoint().getUri().toString()
					.equals("internal:kafka")) {
				internal = "1";
			}
			return client
					.query("NOTIFY regsubscriptionchannel, '" + request.getId() + seperator + request.getTenant()
							+ seperator + request.getRequestType() + seperator + SYNC_ID + seperator + internal + "'")
					.execute().onItem().transformToUni(r -> Uni.createFrom().voidItem());
		});

	}

}
