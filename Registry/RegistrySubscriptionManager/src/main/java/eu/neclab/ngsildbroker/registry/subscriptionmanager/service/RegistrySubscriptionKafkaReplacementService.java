package eu.neclab.ngsildbroker.registry.subscriptionmanager.service;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.interfaces.TopicListener;
import eu.neclab.ngsildbroker.commons.messagebus.InternalKafkaReplacement;

@Service
@ConditionalOnProperty(prefix = "scorpio.kafka", matchIfMissing = false, name = "enabled", havingValue = "false")
public class RegistrySubscriptionKafkaReplacementService extends RegistrySubscriptionKafkaServiceBase
		implements TopicListener {

	@Value("${scorpio.topics.registry}")
	private String registryTopic;

	@Value("${scorpio.topics.internalregsub}")
	private String subscriptionTopic;

	@Autowired
	InternalKafkaReplacement internalKafkaReplacement;

	@PostConstruct
	private void setup() {
		internalKafkaReplacement.addListener(registryTopic, this);
		internalKafkaReplacement.addListener(subscriptionTopic, this);
	}

	@Override
	public void newMessage(String topic, String key, Object origMessage) {
		if (topic.equals(registryTopic)) {
			if (!(origMessage instanceof BaseRequest)) {
				return;
			}
			handleBaseRequestRegistry((BaseRequest) origMessage, key, System.currentTimeMillis());
		} else if (topic.equals(subscriptionTopic)) {
			if (!(origMessage instanceof SubscriptionRequest)) {
				return;
			}
			handleBaseRequestSubscription((SubscriptionRequest) origMessage, key);
		}
	}

}
