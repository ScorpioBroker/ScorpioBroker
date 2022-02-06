package eu.neclab.ngsildbroker.subscriptionmanager.service;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import eu.neclab.ngsildbroker.commons.datatypes.InternalNotification;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.interfaces.TopicListener;
import eu.neclab.ngsildbroker.commons.messagebus.InternalKafkaReplacement;

@Service
@ConditionalOnProperty(prefix = "scorpio.kafka", matchIfMissing = false, name = "enabled", havingValue = "false")
public class SubscriptionKafkaReplacementService extends SubscriptionKafkaServiceBase implements TopicListener {

	@Value("${scorpio.topics.entity}")
	private String entityTopic;

	@Value("${scorpio.topics.internalnotification}")
	private String internalNotificationTopic;

	@Autowired
	InternalKafkaReplacement internalKafkaReplacement;

	@PostConstruct
	private void setup() {
		internalKafkaReplacement.addListener(entityTopic, this);
		internalKafkaReplacement.addListener(internalNotificationTopic, this);
	}

	@Override
	public void newMessage(String topic, String key, Object origMessage) {
		if (topic.equals(entityTopic)) {
			if (!(origMessage instanceof BaseRequest)) {
				return;
			}
			handleBaseRequestEntity((BaseRequest) origMessage, key, System.currentTimeMillis());
		} else if (topic.equals(internalNotificationTopic)) {
			if (!(origMessage instanceof InternalNotification)) {
				return;
			}
			handleBaseRequestInternalNotification((InternalNotification) origMessage);
		}
	}

}
