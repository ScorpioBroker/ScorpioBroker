package eu.neclab.ngsildbroker.registryhandler.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;

@Service
@ConditionalOnProperty(prefix = "scorpio.registry", name = "autorecording", matchIfMissing = true, havingValue = "active")
public class CSourceKafkaService {
	private static final Logger logger = LoggerFactory.getLogger(CSourceKafkaService.class);

	@Autowired
	CSourceService cSourceService;

	@KafkaListener(topics = "${scorpio.topics.entity}", groupId = "csource")
	public void handleEntity(@Payload BaseRequest message, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key,
			@Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timeStamp) {
		switch (message.getRequestType()) {
		case AppConstants.DELETE_REQUEST:
			cSourceService.handleEntityDelete(message);
			break;
		case AppConstants.UPDATE_REQUEST:
		case AppConstants.CREATE_REQUEST:
		case AppConstants.DELETE_ATTRIBUTE_REQUEST:
		case AppConstants.APPEND_REQUEST:
			cSourceService.handleEntityCreateOrUpdate(message);
			break;
		default:
			break;
		}
	}

}
