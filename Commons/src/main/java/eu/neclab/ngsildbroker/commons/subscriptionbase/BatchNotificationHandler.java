package eu.neclab.ngsildbroker.commons.subscriptionbase;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;

import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.interfaces.NotificationHandler;
import io.smallrye.mutiny.tuples.Tuple3;

public class BatchNotificationHandler {

	private BaseSubscriptionService subService;
	private Timer watchDog = new Timer(true);
	private Map<Integer, MyTask> batchId2WatchTask = Maps.newHashMap();
	private Map<Integer, Integer[]> batchId2Count2BatchSize = Maps.newHashMap();

	int waitTimeForEvac;

	Table<Integer, SubscriptionRequest, Tuple3<NotificationHandler, List<Map<String, Object>>, Integer>> batches = HashBasedTable
			.create();
	private int maxRetries;

	public BatchNotificationHandler(BaseSubscriptionService subService, int waitTimeForEvac, int maxRetries) {
		this.subService = subService;
		this.waitTimeForEvac = waitTimeForEvac;
		this.maxRetries = maxRetries;
	}

	public void addFail(BatchInfo batchInfo) {

		synchronized (batchId2Count2BatchSize) {
			increaseBatch(batchInfo);
		}

	}

	private void increaseBatch(BatchInfo batchInfo) {
		int batchId = batchInfo.getBatchId();
		int batchSize = batchInfo.getBatchSize();
		Integer[] tuple = batchId2Count2BatchSize.get(batchId);
		if (tuple == null) {
			tuple = new Integer[] { 1, batchSize };
			batchId2Count2BatchSize.put(batchId, tuple);
		} else {
			tuple[0] = tuple[0] + 1;
		}
		if (tuple[0] >= tuple[1]) {
			finalizeBatch(batchId);
		}

	}

	public void addDataToBatch(BatchInfo batchInfo, NotificationHandler handler,
			SubscriptionRequest subscriptionRequest, List<Map<String, Object>> dataList, int triggerReason) {

		synchronized (batchId2Count2BatchSize) {
			int batchId = batchInfo.getBatchId();
			Tuple3<NotificationHandler, List<Map<String, Object>>, Integer> entry = batches.get(batchId,
					subscriptionRequest);
			if (entry == null) {
				entry = Tuple3.of(handler, dataList, triggerReason);
				batches.put(batchId, subscriptionRequest, entry);
			} else {
				entry.getItem2().addAll(dataList);
			}
			MyTask task = batchId2WatchTask.get(batchId);
			if (task == null) {
				task = new MyTask(batchId);
				batchId2WatchTask.put(batchId, task);
				watchDog.schedule(task, waitTimeForEvac);
			}

			increaseBatch(batchInfo);
		}

	}

	public void finalizeBatch(int batchId) {
		Map<SubscriptionRequest, Tuple3<NotificationHandler, List<Map<String, Object>>, Integer>> entries = batches
				.row(batchId);
		for (Entry<SubscriptionRequest, Tuple3<NotificationHandler, List<Map<String, Object>>, Integer>> entry : entries
				.entrySet()) {
			Tuple3<NotificationHandler, List<Map<String, Object>>, Integer> value = entry.getValue();
			value.getItem1().notify(subService.getNotification(entry.getKey(), value.getItem2(), value.getItem3()),
					entry.getKey(), maxRetries);
		}
		batches.row(batchId).clear();
		MyTask task = batchId2WatchTask.remove(batchId);
		if (task != null) {
			task.cancel();
		}
		batchId2Count2BatchSize.remove(batchId);
	}

	private class MyTask extends TimerTask {
		private int batchId;

		public MyTask(int batchId) {
			this.batchId = batchId;
		}

		@Override
		public void run() {
			finalizeBatch(batchId);
		}

	}
}
