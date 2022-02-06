package eu.neclab.ngsildbroker.registryhandler.service;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.interfaces.TopicListener;
import eu.neclab.ngsildbroker.commons.messagebus.InternalKafkaReplacement;

@Service
@ConditionalOnProperty(prefix = "scorpio.kafka", matchIfMissing = false, name = "enabled", havingValue = "false")
public class CSourceKafkaReplacementService extends CSourceKafkaServiceBase implements TopicListener {

	@Value("${scorpio.topics.entity}")
	private String topic;
	@Autowired
	InternalKafkaReplacement internalKafkaReplacement;

	@PostConstruct
	private void setup() {
		internalKafkaReplacement.addListener(topic, this);
	}

	@Override
	public void newMessage(String topic, String key, Object origMessage) {
		if (!(origMessage instanceof BaseRequest)) {
			return;
		}
		BaseRequest message = (BaseRequest) origMessage;
		handleBaseRequest(message, key);
	}

}
