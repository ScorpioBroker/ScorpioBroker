package eu.neclab.ngsildbroker.historymanager.service;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.AppendHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.HistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.UpdateHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.interfaces.TopicListener;
import eu.neclab.ngsildbroker.commons.messagebus.InternalKafkaReplacement;

@Service
@ConditionalOnProperty(prefix = "scorpio.kafka", matchIfMissing = false, name = "enabled", havingValue = "false")
public class HistoryKafkaReplacementService implements TopicListener {

	private static final Logger logger = LoggerFactory.getLogger(HistoryKafkaReplacementService.class);

	@Value("${scorpio.topics.entity}")
	private String topic;
	@Autowired
	InternalKafkaReplacement internalKafkaReplacement;
	@Autowired
	HistoryService historyService;

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
		HistoryEntityRequest request;
		try {
			switch (message.getRequestType()) {
			case AppConstants.APPEND_REQUEST:
				logger.debug("Append got called: " + key);
				request = new AppendHistoryEntityRequest(message);
				break;
			case AppConstants.CREATE_REQUEST:
				logger.debug("Create got called: " + key);
				request = new CreateHistoryEntityRequest(message);
				break;
			case AppConstants.UPDATE_REQUEST:
				logger.debug("Update got called: " + key);
				request = new UpdateHistoryEntityRequest(message);
				break;
			case AppConstants.DELETE_REQUEST:
				logger.debug("Delete got called: " + key);
				request = null;
				break;
			default:
				request = null;
				break;
			}
			if (request != null) {
				historyService.handleRequest(request);
			}
		} catch (Exception e) {
			logger.error("Internal history recording failed", e.getMessage());
		}

	}

}
