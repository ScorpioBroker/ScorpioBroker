package eu.neclab.ngsildbroker.commons.messagebus;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.HashMultimap;

import eu.neclab.ngsildbroker.commons.interfaces.TopicListener;

public class InternalKafkaReplacement {

	private HashMultimap<String, TopicListener> topic2Listeners = HashMultimap.create();
	private HashMap<TopicListener, ThreadPoolExecutor> listener2ThreadExecutor = new HashMap<TopicListener, ThreadPoolExecutor>();

	public void addListener(String topic, TopicListener topicListener) {
		topic2Listeners.put(topic, topicListener);
		listener2ThreadExecutor.put(topicListener,
				new ThreadPoolExecutor(1, 1, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>()));
	}

	public void removeListener(String topic, TopicListener topicListener) {
		topic2Listeners.remove(topic, topicListener);
		listener2ThreadExecutor.remove(topicListener);
	}

	public synchronized void newMessage(String topic, String key, Object object) {
		Set<TopicListener> listeners = topic2Listeners.get(topic);
		for (TopicListener listener : listeners) {
			ThreadPoolExecutor executor = listener2ThreadExecutor.get(listener);
			executor.execute(new Runnable() {

				@Override
				public void run() {
					listener.newMessage(topic, key, object);

				}
			});
		}
	}
}
