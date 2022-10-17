package eu.neclab.ngsildbroker.commons.subscriptionbase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.Lists;

import eu.neclab.ngsildbroker.commons.datatypes.Notification;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.interfaces.NotificationHandler;

public class IntervalNotificationHandler {

	static final Logger logger = LoggerFactory.getLogger(IntervalNotificationHandler.class);
	private NotificationHandler notificationHandler;
	private HashMap<String, TimerTask> id2TimerTask = new HashMap<String, TimerTask>();
	private Timer executor = new Timer(true);
	private SubscriptionInfoDAOInterface infoDAO;
	private Notification baseNotification;

	public IntervalNotificationHandler(NotificationHandler notificationHandler, SubscriptionInfoDAOInterface infoDAO,
			Notification baseNotification) {
		this.notificationHandler = notificationHandler;
		this.infoDAO = infoDAO;
		this.baseNotification = baseNotification;
	}

	public void addSub(SubscriptionRequest subscriptionRequest) {
		MyTimer timer = new MyTimer(subscriptionRequest, Notification.copy(baseNotification));
		synchronized (id2TimerTask) {
			id2TimerTask.put(subscriptionRequest.getSubscription().getId().toString(), timer);
		}
		executor.schedule(timer, 0, subscriptionRequest.getSubscription().getTimeInterval() * 1000);
	}

	public void removeSub(String subId) {
		synchronized (id2TimerTask) {
			if (id2TimerTask.containsKey(subId)) {
				id2TimerTask.get(subId).cancel();
				id2TimerTask.remove(subId);
			}
		}
	}

	private class MyTimer extends TimerTask {

		private SubscriptionRequest subscriptionRequest;
		private Notification notification;

		public MyTimer(SubscriptionRequest subscriptionRequest, Notification base) {
			this.subscriptionRequest = subscriptionRequest;
			this.notification = base;
			notification.setSubscriptionId(subscriptionRequest.getSubscription().getId());
			notification.setContext(subscriptionRequest.getContext());
		}

		@SuppressWarnings("unchecked")
		public void run() {
			if (!subscriptionRequest.isActive()) {
				return;
			}
			try {
				List<String> entries = infoDAO.getEntriesFromSub(subscriptionRequest);
				if (entries == null) {
					entries = Lists.newArrayList();
				}
				List<Map<String, Object>> dataList = new ArrayList<Map<String, Object>>();
				for (String entry : entries) {
					dataList.add((Map<String, Object>) JsonUtils.fromString(entry));
				}
				notification.setNotifiedAt(System.currentTimeMillis());
				notification.setData(dataList);
				notificationHandler.notify(notification, subscriptionRequest);
			} catch (Exception e) {
				logger.error("Failed to read database entry");
			}
		}

	}
}
