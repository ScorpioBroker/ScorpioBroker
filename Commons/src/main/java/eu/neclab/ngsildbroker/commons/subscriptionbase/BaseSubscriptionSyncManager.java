package eu.neclab.ngsildbroker.commons.subscriptionbase;

import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;

import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.datatypes.AliveAnnouncement;
import eu.neclab.ngsildbroker.commons.datatypes.HandingOfAnnouncement;
import eu.neclab.ngsildbroker.commons.datatypes.TakingAnnouncement;
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

	;

	@Value("${scorpio.sync.announcement-time:200}")
	int announcementTime;

	@Value("${scorpio.sync.check-time:1000}")
	int checkTime;

	AliveAnnouncement INSTANCE_ID = new AliveAnnouncement(UUID.randomUUID().toString());

	@PostConstruct
	public void setup() {
		currentInstances.add(INSTANCE_ID.getId());
		lastInstances.add(INSTANCE_ID.getId());
		startSyncing();
		// TODO FIGURE OUT HOW TO SET RENTENTION TIME FOR THE TOPIC
	}

	private void startSyncing() {
		executor.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				kafkaTemplate.send(getAliveTopic(), "alive", INSTANCE_ID);
			}
		}, 0, announcementTime);

		executor.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				synchronized (currentInstances) {
					if (currentInstances.equals(lastInstances)) {
						recalculateSubscriptions();
					}
					lastInstances = currentInstances;
				}
			}
		}, 0, checkTime);

	}

	protected abstract String getAliveTopic();

	protected void listenForAnnouncements(AnnouncementMessage announcement) {
		if (announcement instanceof AliveAnnouncement) {
			synchronized (currentInstances) {
				currentInstances.add(announcement.getId());
			}
		}
		if (announcement instanceof TakingAnnouncement) {

		}
		if (announcement instanceof HandingOfAnnouncement) {

		}
	}

	protected void listenForSubscriptionUpdates(SubscriptionRequest sub, String key) {
		switch (key) {
		case "delete":
			try {
				subscriptionService.unsubscribe(sub.getId(), sub.getHeaders(), true);
			} catch (ResponseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			break;
		case "update":
			try {
				subscriptionService.updateSubscription(sub, true);
			} catch (ResponseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			break;
		case "create":
			try {
				subscriptionService.subscribe(sub, true);
			} catch (ResponseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			break;
		default:
			break;
		}
	}

	private void recalculateSubscriptions() {
		List<String> sortedInstances = currentInstances.stream().sorted().collect(Collectors.toList());
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
