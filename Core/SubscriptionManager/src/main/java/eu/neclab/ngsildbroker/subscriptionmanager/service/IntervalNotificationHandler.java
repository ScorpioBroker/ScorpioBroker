package eu.neclab.ngsildbroker.subscriptionmanager.service;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.datatypes.Notification;
import eu.neclab.ngsildbroker.commons.interfaces.NotificationHandler;
import eu.neclab.ngsildbroker.commons.ldcontext.ContextResolverBasic;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;

public class IntervalNotificationHandler {

	private ArrayListMultimap<String, Notification> id2Data = ArrayListMultimap.create();
	private HashMap<String, URI> id2Callback = new HashMap<String, URI>();
	private HashMap<String, TimerTask> id2TimerTask = new HashMap<String, TimerTask>();
	private Timer executor = new Timer(true);
	private ContextResolverBasic contextResolver;
	
	private NotificationHandler notificationHandler;
	private ArrayListMultimap<String, Object> subId2Context;

	public IntervalNotificationHandler(ContextResolverBasic contextResolver, NotificationHandler notificationHandler, ArrayListMultimap<String, Object> subId2Context) {
		this.contextResolver = contextResolver;
		this.notificationHandler = notificationHandler;
		this.subId2Context = subId2Context;
	}


	public void addSub(String subId, long interval, URI callback, String acceptHeader) {
		id2Callback.put(subId, callback);
		TimerTask temp = new TimerTask() {

			@Override
			public void run() {
				List<Notification> data;
				synchronized (id2Data) {
					data = id2Data.removeAll(subId);
				}
				if (data != null) {
					Notification notification = EntityTools.squashNotifications(data);
					notificationHandler.notify(notification, callback, acceptHeader, subId, subId2Context.get(subId), 0);
				}

			}

		};
		id2TimerTask.put(subId, temp);
		executor.schedule(temp, 0, interval);
	}

	
	public void removeSub(String subId) {
		id2TimerTask.get(subId).cancel();
		id2TimerTask.remove(subId);
		id2Callback.remove(subId);
		id2Data.removeAll(subId);
	}

	public void notify(Notification notification, String subId) {
		synchronized (id2Data) {
			id2Data.put(subId, notification);
		}

	}

}
