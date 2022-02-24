package eu.neclab.ngsildbroker.commons.subscriptionbase;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.admin.AdminClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.stereotype.Service;

import eu.neclab.ngsildbroker.commons.datatypes.SyncAnnouncement;
import eu.neclab.ngsildbroker.commons.interfaces.SyncMessage;

public abstract class SubscriptionSyncManager {

	protected abstract String getSyncTopic();

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Autowired
	BaseSubscriptionService subscriptionService;

	@Autowired
	KafkaTemplate<String, SyncMessage> kafkaTemplate;

	String SYNC_TOPIC = getSyncTopic();

	@Value("{scorpio.sync.timeout}")
	long timeout;

	SyncMessage INSTANCE_ID = new SyncAnnouncement(UUID.randomUUID().toString());

	public void announce() {

		kafkaTemplate.send(SYNC_TOPIC, INSTANCE_ID);

	}

}
