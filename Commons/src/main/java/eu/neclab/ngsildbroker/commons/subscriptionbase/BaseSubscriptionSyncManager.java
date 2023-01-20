package eu.neclab.ngsildbroker.commons.subscriptionbase;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.AliveAnnouncement;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;

public abstract class BaseSubscriptionSyncManager {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

		private Set<String> currentInstances = Sets.newHashSet();

	private Set<String> lastInstances = Sets.newHashSet();

	BaseSubscriptionService subscriptionService;

	private MutinyEmitter<AliveAnnouncement> kafkaSender;

	@ConfigProperty(name = "scorpio.sync.announcement-time", defaultValue = "200")
	int announcementTime;

	@ConfigProperty(name = "scorpio.sync.check-time", defaultValue = "1000")
	int checkTime;

	protected String syncId;

	AliveAnnouncement INSTANCE_ID;

	@PostConstruct
	public void setup() {
		this.kafkaSender = getAliveEmitter();
		this.subscriptionService = getSubscriptionService();
		setSyncId();
		INSTANCE_ID = new AliveAnnouncement(syncId);
	}

	protected abstract BaseSubscriptionService getSubscriptionService();

	protected abstract MutinyEmitter<AliveAnnouncement> getAliveEmitter();

	protected abstract void setSyncId();

	
	@Scheduled(every = "{scorpio.sync.announcement-time}")
	Uni<Void> syncTask() {
		return kafkaSender.send(INSTANCE_ID);
	}

	@Scheduled(every = "{scorpio.sync.check-time}")
	Uni<Void> checkTask() {
		if (!currentInstances.equals(lastInstances)) {
			recalculateSubscriptions();
		}
		lastInstances.clear();
		lastInstances.addAll(currentInstances);
		currentInstances.clear();
		return Uni.createFrom().voidItem();
	}

	protected void listenForAnnouncements(AliveAnnouncement announcement, String key) {
		if (key.equals(syncId)) {
			return;
		}

		synchronized (currentInstances) {
			currentInstances.add(announcement.getId());
		}

	}

	protected void listenForSubscriptionUpdates(SubscriptionRequest sub, String key) {
		if (key.equals(syncId)) {
			return;
		}
		switch (sub.getType()) {
		case AppConstants.DELETE_REQUEST:
			subscriptionService.unsubscribe(sub.getId(), sub.getTenant(), true).onFailure().transform(e -> {
				logger.debug("Failed to forward delete request", e);
				return null;
			}).await().indefinitely();
			break;
		case AppConstants.UPDATE_REQUEST:
			subscriptionService.updateSubscription(sub, true).onFailure().transform(e -> {
				logger.debug("Failed to forward delete request", e);
				return null;
			}).await().indefinitely();
			break;
		case AppConstants.CREATE_REQUEST:
			sub.setActive(false);
			subscriptionService.subscribe(sub, true).onFailure().transform(e -> {
				logger.debug("Failed to forward delete request", e);
				return null;
			}).await().indefinitely();
			break;
		default:
			break;
		}
	}

	private void recalculateSubscriptions() {
		HashSet<String> temp = Sets.newHashSet(currentInstances);
		temp.add(INSTANCE_ID.getId());
		List<String> sortedInstances = temp.stream().sorted().collect(Collectors.toList());
		int myPos = sortedInstances.indexOf(INSTANCE_ID.getId());
		List<String> sortedSubs = subscriptionService.getAllSubscriptionIds();
		int stepRange = sortedSubs.size() / sortedInstances.size();
		int start = myPos * stepRange;
		int end;
		if (myPos == sortedInstances.size() - 1) {
			end = sortedSubs.size();
		} else {
			end = (myPos + 1) * stepRange;
		}
		List<String> mySubs = sortedSubs.subList(start, end);
		subscriptionService.activateSubs(mySubs);
	}

}
