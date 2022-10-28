package eu.neclab.ngsildbroker.commons.subscriptionbase;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;

import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.AliveAnnouncement;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.AnnouncementMessage;

public abstract class BaseSubscriptionSyncManager {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	private Timer executor = new Timer(true);

	private Set<String> currentInstances = Sets.newHashSet();

	private Set<String> lastInstances = Sets.newHashSet();
	@Autowired
	BaseSubscriptionService subscriptionService;

	@Autowired
	KafkaTemplate<String, AnnouncementMessage> kafkaTemplate;

	@Value("${scorpio.sync.announcement-time:200}")
	int announcementTime;

	@Value("${scorpio.sync.check-time:1000}")
	int checkTime;

	protected String syncId;
	
	protected String aliveTopic;

	AliveAnnouncement INSTANCE_ID;

	@PostConstruct
	public void setup() {
		setSyncId();
		setAliveTopic();
		INSTANCE_ID = new AliveAnnouncement(syncId);
		startSyncing();
		// TODO FIGURE OUT HOW TO SET RENTENTION TIME FOR THE TOPIC
	}

	protected abstract void setAliveTopic();

	protected abstract void setSyncId();

	private void startSyncing() {
		executor.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				kafkaTemplate.send(aliveTopic, syncId, INSTANCE_ID);
			}
		}, 0, announcementTime);

		executor.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				synchronized (currentInstances) {
					if (!currentInstances.equals(lastInstances)) {
						recalculateSubscriptions();
					}
					lastInstances.clear();
					lastInstances.addAll(currentInstances);
					currentInstances.clear();
				}
			}
		}, 0, checkTime);

	}

	protected void listenForAnnouncements(AnnouncementMessage announcement, String key) {
		if (key.equals(syncId)) {
			return;
		}
		if (announcement instanceof AliveAnnouncement) {
			synchronized (currentInstances) {
				currentInstances.add(announcement.getId());
			}
		}
//		if (announcement instanceof TakingAnnouncement) {
//
//		}
//		if (announcement instanceof HandingOfAnnouncement) {
//
//		}
	}

	protected void listenForSubscriptionUpdates(SubscriptionRequest sub, String key) {
		if (key.equals(syncId)) {
			return;
		}
		switch (sub.getType()) {
		case AppConstants.DELETE_REQUEST:
			try {
				subscriptionService.unsubscribe(sub.getId(), sub.getHeaders(), true);
			} catch (ResponseException e) {
				logger.debug("Failed to forward delete request", e);
			}
			break;
		case AppConstants.UPDATE_REQUEST:
			try {
				subscriptionService.updateSubscription(sub, true);
			} catch (ResponseException e) {
				logger.debug("Failed to forward update request", e);
			}
			break;
		case AppConstants.CREATE_REQUEST:
			try {
				//sub.setActive(false);
				subscriptionService.subscribe(sub, true);
			} catch (ResponseException e) {
				logger.debug("Failed to forward create request", e);
			}
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
	
	@PreDestroy
	private void destroy() {
		executor.cancel();
	}

}
