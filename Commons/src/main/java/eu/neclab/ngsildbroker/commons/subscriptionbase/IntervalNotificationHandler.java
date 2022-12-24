package eu.neclab.ngsildbroker.commons.subscriptionbase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.neclab.ngsildbroker.commons.datatypes.Notification;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.interfaces.NotificationHandler;
import io.smallrye.mutiny.infrastructure.Infrastructure;

public class IntervalNotificationHandler {

	static final Logger logger = LoggerFactory.getLogger(IntervalNotificationHandler.class);
	private NotificationHandler notificationHandler;
	private HashMap<String, TimerTask> id2TimerTask = new HashMap<String, TimerTask>();
	private Timer executor = new Timer(true);
	private SubscriptionInfoDAOInterface infoDAO;
	private Notification baseNotification;
	private int maxRetries;

	public IntervalNotificationHandler(NotificationHandler notificationHandler, SubscriptionInfoDAOInterface infoDAO,
			Notification baseNotification, int maxRetries) {
		this.notificationHandler = notificationHandler;
		this.infoDAO = infoDAO;
		this.baseNotification = baseNotification;
		this.maxRetries = maxRetries;
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

		public void run() {
			if (!subscriptionRequest.isActive()) {
				return;
			}

			List<Map<String, Object>> data = infoDAO.getEntriesFromSub(subscriptionRequest)
					.runSubscriptionOn(Infrastructure.getDefaultExecutor()).onItem().transform(entries -> {
						List<Map<String, Object>> dataList = new ArrayList<Map<String, Object>>();
						for (Map<String, Object> entry : entries) {
							dataList.add(entry);
						}
						return dataList;
					}).await().indefinitely();
			notification.setNotifiedAt(System.currentTimeMillis());
			notification.setData(data);
			notificationHandler.notify(notification, subscriptionRequest, maxRetries);

//						
//						return true;
//					})).runSubscriptionOn(Infrastructure.getDefaultExecutor()).onFailure().recoverWithItem(e -> {
//						logger.error("Failed to read database entry", e);
//						return true;
//					}).await().indefinitely();

		}

	}
}
