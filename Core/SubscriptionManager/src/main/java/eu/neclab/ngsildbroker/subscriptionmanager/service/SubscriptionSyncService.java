package eu.neclab.ngsildbroker.subscriptionmanager.service;

import java.util.UUID;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.interfaces.AnnouncementMessage;
import eu.neclab.ngsildbroker.commons.subscriptionbase.BaseSubscriptionSyncManager;

@Service
public class SubscriptionSyncService extends BaseSubscriptionSyncManager {

	public static final String SYNC_ID = UUID.randomUUID().toString();

	@Value("${scorpio.topics.subalive}")
	private String SUB_ALIVE_TOPIC;

	@KafkaListener(topics = "${scorpio.topics.subsync}", groupId = "${random.uuid}")
	private void listenForSubs(@Payload SubscriptionRequest message,
			@Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key) {
		listenForSubscriptionUpdates(message, key);
	}

	@KafkaListener(topics = "${scorpio.topics.subalive}", groupId = "${random.uuid}")
	private void listenForAlive(@Payload AnnouncementMessage message,
			@Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key) {
		listenForAnnouncements(message, key);
	}

	@Override
	protected void setSyncId() {
		this.syncId = SubscriptionSyncService.SYNC_ID;
	}

	@Override
	protected void setAliveTopic() {
		this.aliveTopic = SUB_ALIVE_TOPIC;
	}

}
