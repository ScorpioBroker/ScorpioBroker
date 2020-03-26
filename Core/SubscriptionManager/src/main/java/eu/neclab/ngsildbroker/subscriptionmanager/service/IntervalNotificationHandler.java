package eu.neclab.ngsildbroker.subscriptionmanager.service;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.KafkaHeaders;

import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.datatypes.Entity;
import eu.neclab.ngsildbroker.commons.datatypes.Notification;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.interfaces.NotificationHandler;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;

public class IntervalNotificationHandler {

	private ArrayListMultimap<String, Notification> id2Data = ArrayListMultimap.create();
	private HashMap<String, URI> id2Callback = new HashMap<String, URI>();
	private HashMap<String, TimerTask> id2TimerTask = new HashMap<String, TimerTask>();
	private Timer executor = new Timer(true);

	String requestTopic;
	String queryResultTopic;
	ReplyingKafkaTemplate<String, byte[], byte[]> kafkaTemplate;

	private NotificationHandler notificationHandler;
	private ParamsResolver resolver;

	public IntervalNotificationHandler(NotificationHandler notificationHandler,
			ReplyingKafkaTemplate<String, byte[], byte[]> kafkaTemplate, String queryResultTopic, String requestTopic,
			ParamsResolver resolver) {
		this.requestTopic = requestTopic;
		this.queryResultTopic = queryResultTopic;
		this.kafkaTemplate = kafkaTemplate;
		this.notificationHandler = notificationHandler;
		this.resolver = resolver;
	}

	public void addSub(SubscriptionRequest subscriptionRequest) {
		MyTimer timer = new MyTimer(subscriptionRequest);
		id2TimerTask.put(subscriptionRequest.getSubscription().getId().toString(), timer);
		executor.schedule(timer, 0, subscriptionRequest.getSubscription().getTimeInterval() * 1000);
	}

	public List<String> getFromStorageManager(String storageManagerQuery) throws Exception {
		// create producer record
		// logger.trace("getFromStorageManager() :: started");
		ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>(requestTopic,
				storageManagerQuery.getBytes());
		// set reply topic in header
		record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, queryResultTopic.getBytes()));
		RequestReplyFuture<String, byte[], byte[]> sendAndReceive = kafkaTemplate.sendAndReceive(record);
		// get consumer record
		ConsumerRecord<String, byte[]> consumerRecord = sendAndReceive.get();
		// read from byte array
		ByteArrayInputStream bais = new ByteArrayInputStream(consumerRecord.value());
		DataInputStream in = new DataInputStream(bais);
		List<String> entityList = new ArrayList<String>();
		while (in.available() > 0) {
			entityList.add(in.readUTF());
		}
		// return consumer value
		// logger.trace("getFromStorageManager() :: completed");
		return entityList;
	}

	public void removeSub(String subId) {
		if (id2TimerTask.containsKey(subId)) {
			id2TimerTask.get(subId).cancel();
			id2TimerTask.remove(subId);
		}

	}

	private class MyTimer extends TimerTask {

		private SubscriptionRequest subscriptionRequest;
		private Subscription subscription;
		private ArrayList<String> paramStrings;

		public MyTimer(SubscriptionRequest subscriptionRequest) {
			this.subscriptionRequest = subscriptionRequest;
			this.subscription = subscriptionRequest.getSubscription();
			List<QueryParams> params = resolver.getQueryParamsFromSubscription(subscription);
			this.paramStrings = new ArrayList<String>();
			for (QueryParams param : params) {
				paramStrings.add(DataSerializer.toJson(param));
			}
		}

		public void run() {

			ArrayList<Entity> entities = new ArrayList<Entity>();
			try {
				for (String param : paramStrings) {
					for (String entityString : getFromStorageManager(param)) {
						entities.add(DataSerializer.getEntity(entityString));
					}
				}
				Notification notification = new Notification(EntityTools.getRandomID("notification:"),
						System.currentTimeMillis(), subscription.getId(), entities, null, null, 0, true);
				notificationHandler.notify(notification, subscription.getNotification().getEndPoint().getUri(),
						subscription.getNotification().getEndPoint().getAccept(), subscription.getId().toString(),
						subscriptionRequest.getContext(), 0, null);
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		};

	}
}
