package eu.neclab.ngsildbroker.subscriptionmanager.messaging;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.apache.camel.CamelContext;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment;
import org.eclipse.microprofile.reactive.messaging.Acknowledgment.Strategy;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.AliveAnnouncement;
import eu.neclab.ngsildbroker.commons.datatypes.SyncMessage;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.SubscriptionRequest;
import eu.neclab.ngsildbroker.subscriptionmanager.service.SubscriptionService;
import io.quarkus.arc.profile.UnlessBuildProfile;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;

@Singleton
@UnlessBuildProfile("in-memory")
public class SubscriptionSyncService {

	public static final String SYNC_ID = UUID.randomUUID().toString();

	AliveAnnouncement INSTANCE_ID;

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	private Set<String> currentInstances = Sets.newHashSet();

	private Set<String> lastInstances = Sets.newHashSet();

	@Inject
	@Channel(AppConstants.SUB_ALIVE_CHANNEL)
	MutinyEmitter<AliveAnnouncement> aliveEmitter;

	@Inject
	@Channel(AppConstants.SUB_SYNC_CHANNEL)
	MutinyEmitter<SyncMessage> syncEmitter;

	@Inject
	SubscriptionService subService;


	@PostConstruct
	public void setup() {
		INSTANCE_ID = new AliveAnnouncement(SYNC_ID);
		subService.addSyncService(this);
	}

	@Scheduled(every = "${scorpio.sync.announcement-time}", delayed = "${scorpio.startupdelay}")
	Uni<Void> syncTask() {
		return aliveEmitter.send(INSTANCE_ID);
	}

	@Scheduled(every = "${scorpio.sync.check-time}", delayed = "${scorpio.startupdelay}")
	Uni<Void> checkTask() {
		if (!currentInstances.equals(lastInstances)) {
			recalculateSubscriptions();
		}
		lastInstances.clear();
		lastInstances.addAll(currentInstances);
		currentInstances.clear();
		return Uni.createFrom().voidItem();
	}

	@Incoming(AppConstants.SUB_SYNC_RETRIEVE_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	Uni<Void> listenForSubs(SyncMessage message) {
		String key = message.getSyncId();
		SubscriptionRequest sub = message.getRequest();
		if (key.equals(SYNC_ID)) {
			return Uni.createFrom().voidItem();
		}
		switch (sub.getRequestType()) {
		case AppConstants.DELETE_REQUEST:
			return subService.syncDeleteSubscription(sub);
		case AppConstants.UPDATE_REQUEST:
			return subService.syncUpdateSubscription(sub);
		case AppConstants.CREATE_REQUEST:
			return subService.syncCreateSubscription(sub);
		default:
			return Uni.createFrom().voidItem();
		}
	}

	@Incoming(AppConstants.SUB_ALIVE_RETRIEVE_CHANNEL)
	@Acknowledgment(Strategy.PRE_PROCESSING)
	Uni<Void> listenForAlive(AliveAnnouncement message) {
		System.out.println("[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[[");
		if (message.getId().equals(SYNC_ID)) {
			return Uni.createFrom().voidItem();
		}
		currentInstances.add(message.getId());
		return Uni.createFrom().voidItem();

	}

	private void recalculateSubscriptions() {
		HashSet<String> temp = Sets.newHashSet(currentInstances);
		temp.add(INSTANCE_ID.getId());
		List<String> sortedInstances = temp.stream().sorted().collect(Collectors.toList());
		int myPos = sortedInstances.indexOf(INSTANCE_ID.getId());
		List<String> sortedSubs = subService.getAllSubscriptionIds();
		int stepRange = sortedSubs.size() / sortedInstances.size();
		int start = myPos * stepRange;
		int end;
		if (myPos == sortedInstances.size() - 1) {
			end = sortedSubs.size();
		} else {
			end = (myPos + 1) * stepRange;
		}
		List<String> mySubs = sortedSubs.subList(start, end);
		subService.activateSubs(mySubs);
	}

	public Uni<Void> sync(SubscriptionRequest request) {
		return syncEmitter.send(new SyncMessage(SYNC_ID, request));
	}

}
