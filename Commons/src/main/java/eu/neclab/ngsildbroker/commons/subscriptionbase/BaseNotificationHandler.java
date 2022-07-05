package eu.neclab.ngsildbroker.commons.subscriptionbase;

import java.util.Date;
import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.datatypes.Notification;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.interfaces.NotificationHandler;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;

public abstract class BaseNotificationHandler implements NotificationHandler {

	protected abstract void sendReply(Notification notification, SubscriptionRequest request) throws Exception;

	protected final Logger logger = LoggerFactory.getLogger(this.getClass());
	private HashMap<String, Long> subId2LastReport = new HashMap<String, Long>();
	private ArrayListMultimap<String, Notification> subId2Notifications = ArrayListMultimap.create();
	private Timer executor = new Timer(true);
	private SubscriptionInfoDAOInterface baseSubscriptionInfoDAO;

	public BaseNotificationHandler(SubscriptionInfoDAOInterface baseSubscriptionInfoDAO) {
		this.baseSubscriptionInfoDAO = baseSubscriptionInfoDAO;
	}

	@Override
	public void notify(Notification notification, SubscriptionRequest subscriptionRequest) {
		if (!subscriptionRequest.getSubscription().isActive()) {
			return;
		}
		Subscription subscription = subscriptionRequest.getSubscription();
		Long now = System.currentTimeMillis();
		if (subscription.getThrottling() > 0) {
			synchronized (subId2Notifications) {
				subId2Notifications.put(subscription.getId(), notification);
				Long lastReport = subId2LastReport.get(subscription.getId());
				if (lastReport == null) {
					lastReport = 0l;
				}
				Long delay = 0l;
				Long delta = now - lastReport;
				if (delta < subscription.getThrottling()) {
					delay = delta;
				}
				executor.schedule(new TimerTask() {

					@Override
					public void run() {
						synchronized (subId2Notifications) {
							Notification sendOutNotification = EntityTools
									.squashNotifications(subId2Notifications.removeAll(subscription.getId()));

							Long now = System.currentTimeMillis();
							subId2LastReport.put(subscription.getId(), now);
							reportNotification(subscription, now);
							try {
								logger.trace("Sending notification");
								sendReply(sendOutNotification, subscriptionRequest);
								reportSuccessfulNotification(subscription, now);
								baseSubscriptionInfoDAO.storeSubscription(subscriptionRequest);
							} catch (Exception e) {
								logger.error("Exception ::", e);
								reportFailedNotification(subscription, now);
								baseSubscriptionInfoDAO.storeSubscription(subscriptionRequest);
							}
						}
					}
				}, delay);
			}
		} else {
			try {
				reportNotification(subscription, now);
				sendReply(notification, subscriptionRequest);
				reportSuccessfulNotification(subscription, now);
				baseSubscriptionInfoDAO.storeSubscription(subscriptionRequest);
			} catch (Exception e) {
				logger.error("Exception ::", e);
				reportFailedNotification(subscription, now);
				baseSubscriptionInfoDAO.storeSubscription(subscriptionRequest);
			}
		}

	}

	void reportNotification(Subscription subscription, Long now) {
		subscription.getNotification().setLastNotification(new Date(now));
	}

	void reportFailedNotification(Subscription subscription, Long now) {
		subscription.getNotification().setLastFailedNotification(new Date(now));
	}

	void reportSuccessfulNotification(Subscription subscription, Long now) {
		subscription.getNotification().setLastSuccessfulNotification(new Date(now));
	}

}
