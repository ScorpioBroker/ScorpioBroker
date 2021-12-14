package eu.neclab.ngsildbroker.registryhandler.service;

import java.util.HashMap;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.datatypes.CSourceNotification;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.interfaces.CSourceNotificationHandler;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;

public class CSourceIntervalNotificationHandler {

	private ArrayListMultimap<String, CSourceNotification> id2Data = ArrayListMultimap.create();
	
	private HashMap<String, TimerTask> id2TimerTask = new HashMap<String, TimerTask>();
	private Timer executor = new Timer(true);
	

	private CSourceNotificationHandler notificationHandler;

	public CSourceIntervalNotificationHandler(CSourceNotificationHandler notificationHandler) {
		this.notificationHandler = notificationHandler;
	}


	public void addSub(Subscription sub, long interval) {
		
		TimerTask temp = new TimerTask() {

			@Override
			public void run() {
				List<CSourceNotification> data;
				synchronized (id2Data) {
					data = id2Data.removeAll(sub.getId().toString());
				}
				if (data != null) {
					List<CSourceNotification> notifications = EntityTools.squashCSourceNotifications(data);
					for(CSourceNotification notification: notifications) {
						notificationHandler.notify(notification, sub);
					}
				}

			}

		};
		id2TimerTask.put(sub.getId().toString(), temp);
		executor.schedule(temp, 0, interval);
	}

	
	public void removeSub(String subId) {
		id2TimerTask.get(subId).cancel();
		id2TimerTask.remove(subId);
		id2Data.removeAll(subId);
	}

	public void notify(CSourceNotification notification, Subscription sub) { 
		synchronized (id2Data) {
			id2Data.put(sub.getId().toString(), notification);
		}

	}
}
