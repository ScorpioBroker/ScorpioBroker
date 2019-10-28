package eu.neclab.ngsildbroker.registryhandler.service;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;

import eu.neclab.ngsildbroker.commons.datatypes.CSourceRegistration;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.enums.TriggerReason;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.ngsiqueries.QueryParser;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaOps;
import eu.neclab.ngsildbroker.registryhandler.config.CSourceConsumerChannel;
import eu.neclab.ngsildbroker.registryhandler.config.CSourceProducerChannel;
import eu.neclab.ngsildbroker.registryhandler.controller.RegistryController;
import eu.neclab.ngsildbroker.registryhandler.repository.CSourceDAO;

@Service
public class CSourceService {

	private final static Logger logger = LoggerFactory.getLogger(RegistryController.class);
//	public static final Gson GSON = DataSerializer.GSON;

	@Value("${bootstrap.servers}")
	String BOOTSTRAP_SERVERS;

	@Autowired
	KafkaOps operations;
	@Autowired
	ObjectMapper objectMapper;

	@Autowired
	CSourceDAO csourceDAO;

	@Autowired
	CSourceSubscriptionService csourceSubService;

	@Autowired
	QueryParser queryParser;

	@Value("${csource.source.topic}")
	String CSOURCE_TOPIC;

	private final CSourceProducerChannel producerChannels;
	@SuppressWarnings("unused")
	// TODO check to remove
	private final CSourceConsumerChannel consumerChannels;
	HashMap<String, TimerTask> regId2TimerTask = new HashMap<String, TimerTask>();
	Timer watchDog = new Timer(true);

	CSourceService(CSourceProducerChannel producerChannels, CSourceConsumerChannel consumerChannels) {
		this.consumerChannels = consumerChannels;
		this.producerChannels = producerChannels;
	}

	public List<JsonNode> getCSourceRegistrations() throws ResponseException, IOException, Exception {
		logger.trace("getAll() ::");
		Map<String, byte[]> records = operations.pullFromKafka(this.CSOURCE_TOPIC);
		Map<String, JsonNode> entityMap = new HashMap<String, JsonNode>();
		JsonNode entityJsonBody = objectMapper.createObjectNode();
		byte[] result = null;
		String key = null;

		for (String recordKey : records.keySet()) {
			result = records.get(recordKey);
			key = recordKey;
			entityJsonBody = objectMapper.readTree(result);
			if (!entityJsonBody.isNull())
				entityMap.put(key, entityJsonBody);
		}
		return new ArrayList<JsonNode>(entityMap.values());
	}

	// private List<String> getParamsList(String types) {
	// if (types != null) {
	// return Stream.of(types.split(",")).collect(Collectors.toList());
	// }
	// return null;
	// }
	//
	// private boolean filterForParams(List<String> params, String matchEntity) {
	// if (params == null)
	// return true;
	// if (!params.contains(matchEntity)) {
	// return false;
	// }
	// return true;
	// }
	//
	// private boolean filterForParamsPatterns(List<String> idPatterns, String
	// matchEntity) {
	// if (idPatterns == null)
	// return true;
	// for (String idPattern : idPatterns) {
	// if (Pattern.compile(idPattern).matcher(matchEntity).matches()) {
	// return true;
	// }
	// }
	// return false;
	// }

	public CSourceRegistration getCSourceRegistrationById(String registrationId) throws ResponseException, Exception {
		if (registrationId == null) {
			throw new ResponseException(ErrorType.BadRequestData);
		}
		byte[] csourceBytes = operations.getMessage(registrationId, this.CSOURCE_TOPIC);
		if (csourceBytes == null) {
			throw new ResponseException(ErrorType.NotFound);
		}
		JsonNode entityJsonBody = objectMapper.createObjectNode();
		entityJsonBody = objectMapper.readTree(csourceBytes);
		if (entityJsonBody.isNull()) {
			throw new ResponseException(ErrorType.NotFound);
		}
		return DataSerializer.getCSourceRegistration(objectMapper.writeValueAsString(entityJsonBody));
	}

	public boolean updateCSourceRegistration(String registrationId, String payload)
			throws ResponseException, Exception {
		MessageChannel messageChannel = producerChannels.csourceWriteChannel();
		if (registrationId == null) {
			throw new ResponseException(ErrorType.BadRequestData);
		}
		byte[] csourceBytes = operations.getMessage(registrationId, this.CSOURCE_TOPIC);
		if (csourceBytes == null) {
			throw new ResponseException(ErrorType.ResourceNotFound);
		}
		// original message in kafka.
		JsonNode entityJsonBody = objectMapper.createObjectNode();
		entityJsonBody = objectMapper.readTree(csourceBytes);
		CSourceRegistration prevCSourceRegistration = DataSerializer.getCSourceRegistration(entityJsonBody.toString());
		logger.debug("Previous CSource Registration:: " + prevCSourceRegistration);

		CSourceRegistration updateCS = DataSerializer.getCSourceRegistration(payload);

		CSourceRegistration newCSourceRegistration = prevCSourceRegistration.update(updateCS);

		synchronized (this) {
			TimerTask task = regId2TimerTask.get(registrationId.toString());
			if (task != null) {
				task.cancel();
			}
			this.csourceTimerTask(newCSourceRegistration);
		}
		csourceSubService.checkSubscriptions(prevCSourceRegistration, newCSourceRegistration);
		this.operations.pushToKafka(messageChannel, registrationId.getBytes(),
				DataSerializer.toJson(newCSourceRegistration).getBytes());
		return true;
	}

	public URI registerCSource(CSourceRegistration csourceRegistration) throws ResponseException, Exception {
		MessageChannel messageChannel = producerChannels.csourceWriteChannel();
		String id;
		URI idUri = csourceRegistration.getId();
		if (idUri == null) {
			idUri = generateUniqueRegId(csourceRegistration);
			csourceRegistration.setId(idUri);

		}
		id = idUri.toString();

		if (csourceRegistration.getType() == null) {
			logger.error("Invalid type!");
			throw new ResponseException(ErrorType.BadRequestData);
		}
		if (!isValidURL(csourceRegistration.getEndpoint().toString())) {
			logger.error("Invalid endpoint URL!");
			throw new ResponseException(ErrorType.BadRequestData);
		}
		if (csourceRegistration.getInformation() == null) {
			logger.error("Information is empty!");
			throw new ResponseException(ErrorType.BadRequestData);
		}
		if (csourceRegistration.getExpires() != null && !isValidFutureDate(csourceRegistration.getExpires())) {
			logger.error("Invalid expire date!");
			throw new ResponseException(ErrorType.BadRequestData);
		}

		if (this.operations.isMessageExists(id, this.CSOURCE_TOPIC)) {
			byte[] messageBytes = this.operations.getMessage(id, this.CSOURCE_TOPIC);
			JsonNode messgeJson = objectMapper.createObjectNode();
			messgeJson = objectMapper.readTree(messageBytes);
			if (!messgeJson.isNull()) {
				throw new ResponseException(ErrorType.AlreadyExists);
			}
		}

		// TODO: [check for valid identifier (id)]
		operations.pushToKafka(messageChannel, id.getBytes(), DataSerializer.toJson(csourceRegistration).getBytes());

		this.csourceTimerTask(csourceRegistration);
		if (!csourceRegistration.isInternal()) {
			csourceSubService.checkSubscriptions(csourceRegistration, TriggerReason.newlyMatching);
		}

		return idUri;
	}

	private URI generateUniqueRegId(CSourceRegistration csourceRegistration) {

		try {

			String key = "urn:ngsi-ld:csourceregistration:" + csourceRegistration.hashCode();
			while (this.operations.isMessageExists(key, this.CSOURCE_TOPIC)) {
				key = key + "1";
			}
			return new URI(key);
		} catch (URISyntaxException e) {
			// Left empty intentionally should never happen
			throw new AssertionError();
		}
	}

	public void csourceTimerTask(CSourceRegistration csourceReg) {
		if (csourceReg.getExpires() != null) {
			TimerTask cancel = new TimerTask() {
				@Override
				public void run() {
					try {
						synchronized (this) {
							deleteCSourceRegistration(csourceReg.getId().toString());
						}
					} catch (Exception e) {
						logger.error("Timer Task -> Exception while expiring residtration :: ", e);
					}
				}
			};
			regId2TimerTask.put(csourceReg.getId().toString(), cancel);
			watchDog.schedule(cancel, csourceReg.getExpires().getTime() - System.currentTimeMillis());
		}
	}

	public boolean deleteCSourceRegistration(String registrationId) throws ResponseException, Exception {
		MessageChannel messageChannel = producerChannels.csourceWriteChannel();
		if (registrationId == null) {
			throw new ResponseException(ErrorType.BadRequestData);
		}
		byte[] originalJson = this.operations.getMessage(registrationId, this.CSOURCE_TOPIC);
		if (originalJson == null)
			throw new ResponseException(ErrorType.NotFound);
		CSourceRegistration csourceRegistration = objectMapper.readValue(originalJson, CSourceRegistration.class);
		this.csourceSubService.checkSubscriptions(csourceRegistration, TriggerReason.noLongerMatching);
		this.operations.pushToKafka(messageChannel, registrationId.getBytes(), "null".getBytes());
		return true;
		// TODO: [push to other DELETE TOPIC]
	}

	// for testing
	@StreamListener(CSourceConsumerChannel.csourceReadChannel)
	public void handleEntityCreate(Message<?> message) {
		String payload = (String) message.getPayload();
		String key = new String((byte[]) message.getHeaders().get(KafkaHeaders.RECEIVED_MESSAGE_KEY));
		logger.trace("key received ::::: " + key);
		logger.trace("Received message: {} :::: " + payload);
	}

	@StreamListener(CSourceConsumerChannel.contextRegistryReadChannel)
	public void handleEntityRegistration(Message<?> message) {
		CSourceRegistration csourceRegistration = DataSerializer.getCSourceRegistration((String) message.getPayload());
		// objectMapper.readValue((byte[]) message.getPayload(),
		// CSourceRegistration.class);
		csourceRegistration.setInternal(true);
		try {
			this.registerCSource(csourceRegistration);
		} catch (Exception e) {
			logger.trace("Failed to register csource " + csourceRegistration.getId().toString(), e);
		}
	}

	private static boolean isValidURL(String urlString) {
		URL url;
		try {
			url = new URL(urlString);
			url.toURI();
			return true;
		} catch (Exception e) {
			// put logger
		}
		return false;
	}

	// return true for future date validation
	private boolean isValidFutureDate(Date date) {
		LocalDateTime currentDate = LocalDateTime.now();
		LocalDateTime expires = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
		return !expires.isBefore(currentDate);
	}

	@KafkaListener(topics = "${csource.query.topic}", groupId = "csourceQueryHandler")
	@SendTo
	// @SendTo("QUERY_RESULT") // for tests without QueryManager
	public byte[] handleContextQuery(@Payload byte[] message) throws Exception {
		logger.trace("handleContextQuery() :: started");
		String payload = new String((byte[]) message);
		logger.debug("Received message: " + payload);
		String resultPayload = "";
		try {
			QueryParams qp = DataSerializer.getQueryParams(payload);
			List<String> csourceList = csourceDAO.queryExternalCsources(qp);
			resultPayload = csourceDAO.getListAsJsonArray(csourceList);
		} catch (Exception e) {
			e.printStackTrace();
		}
		logger.trace("Pushing result to Kafka... ");
		logger.trace("handleContextQuery() :: completed");
		return resultPayload.getBytes();
	}

}
