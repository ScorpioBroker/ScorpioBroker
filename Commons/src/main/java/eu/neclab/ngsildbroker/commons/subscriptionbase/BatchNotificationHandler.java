package eu.neclab.ngsildbroker.commons.subscriptionbase;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;

import org.springframework.beans.factory.annotation.Value;

import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.interfaces.NotificationHandler;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuples;

public class BatchNotificationHandler {

	private BaseSubscriptionService subService;
	private Timer watchDog = new Timer(true);
	private Map<Integer, MyTask> batchId2WatchTask = Maps.newHashMap();

	@Value("${scorpio.subscription.batchevactime:300000}")
	int waitTimeForEvac;

	Table<Integer, SubscriptionRequest, Tuple3<NotificationHandler, List<Map<String, Object>>, Integer>> batches = HashBasedTable
			.create();

	public BatchNotificationHandler(BaseSubscriptionService subService) {
		this.subService = subService;
	}

	public void addDataToBatch(int batchId, NotificationHandler handler, SubscriptionRequest subscriptionRequest,
			List<Map<String, Object>> dataList, int triggerReason) {
		Tuple3<NotificationHandler, List<Map<String, Object>>, Integer> entry = batches.get(batchId,
				subscriptionRequest);
		if (entry == null) {
			entry = Tuples.of(handler, dataList, triggerReason);
			batches.put(batchId, subscriptionRequest, entry);
		} else {
			entry.getT2().addAll(dataList);
		}
		MyTask task = batchId2WatchTask.get(batchId);
		if (task == null) {
			task = new MyTask(batchId);
		} else {
			task.cancel();
		}
		watchDog.schedule(task, 60000);

	}

	public void finalizeBatch(int batchId) {
		Map<SubscriptionRequest, Tuple3<NotificationHandler, List<Map<String, Object>>, Integer>> entries = batches
				.row(batchId);
		for (Entry<SubscriptionRequest, Tuple3<NotificationHandler, List<Map<String, Object>>, Integer>> entry : entries
				.entrySet()) {
			Tuple3<NotificationHandler, List<Map<String, Object>>, Integer> value = entry.getValue();
			value.getT1().notify(subService.getNotification(entry.getKey(), value.getT2(), value.getT3()),
					entry.getKey());
		}
		batches.row(batchId).clear();
		MyTask task = batchId2WatchTask.remove(batchId);
		if(task != null) {
			task.cancel();
		}
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
