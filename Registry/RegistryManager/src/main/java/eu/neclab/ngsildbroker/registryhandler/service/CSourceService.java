package eu.neclab.ngsildbroker.registryhandler.service;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ArrayListMultimap;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.AppendCSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.CSourceRegistration;
import eu.neclab.ngsildbroker.commons.datatypes.CSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.CreateCSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.DeleteCSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.enums.TriggerReason;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.ngsiqueries.QueryParser;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.stream.service.KafkaOps;
import eu.neclab.ngsildbroker.commons.tenant.TenantAwareDataSource;
import eu.neclab.ngsildbroker.commons.tenant.TenantContext;
import eu.neclab.ngsildbroker.registryhandler.config.CSourceProducerChannel;
import eu.neclab.ngsildbroker.registryhandler.config.StartupConfig;
import eu.neclab.ngsildbroker.registryhandler.controller.RegistryController;
import eu.neclab.ngsildbroker.registryhandler.repository.CSourceDAO;
import eu.neclab.ngsildbroker.registryhandler.repository.CSourceInfoDAO;

@Service
public class CSourceService {

	private final static Logger logger = LoggerFactory.getLogger(RegistryController.class);
	// public static final Gson GSON = DataSerializer.GSON;

	@Value("${bootstrap.servers}")
	String BOOTSTRAP_SERVERS;

	@Autowired
	@Qualifier("rmops")
	KafkaOps operations;
	@Autowired
	ObjectMapper objectMapper;

	@Autowired
	StartupConfig startupConfig;

	@Autowired
	@Qualifier("rmcsourcedao")
	CSourceDAO csourceDAO;

	@Autowired
	TenantAwareDataSource tenantAwareDataSource;

	@Autowired
	CSourceInfoDAO csourceInfoDAO;

	@Autowired
	CSourceSubscriptionService csourceSubService;

	@Autowired
	@Qualifier("rmqueryParser")
	QueryParser queryParser;

	@Value("${csource.source.topic}")
	String CSOURCE_TOPIC;

	@Value("${csource.directdb:true}")
	boolean directDB = true;

	private final CSourceProducerChannel producerChannels;
	private Set<String> csourceIds = new HashSet<String>();
	private Set<String> tenantcsourceIds = new HashSet<String>();

	HashMap<String, TimerTask> regId2TimerTask = new HashMap<String, TimerTask>();
	Timer watchDog = new Timer(true);
	private ArrayBlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<Runnable>(50000, true);

	ThreadPoolExecutor executor = new ThreadPoolExecutor(20, 50, 600000, TimeUnit.MILLISECONDS, workQueue);

	CSourceService(CSourceProducerChannel producerChannels) {

		this.producerChannels = producerChannels;
	}

	@PostConstruct
	private void loadStoredEntitiesDetails() throws IOException {
		synchronized (this.csourceIds) {
			this.csourceIds = csourceInfoDAO.getAllIds();
		}
	}

	public List<JsonNode> getCSourceRegistrations(String tenant) throws ResponseException, IOException, Exception {
		if (directDB) {
			ArrayList<JsonNode> result = new ArrayList<JsonNode>();
			List<String> regs = csourceInfoDAO.getAllRegistrations(tenant);
			for (String reg : regs) {
				result.add(objectMapper.readTree(reg));
			}
			return result;
		} else {
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

	public CSourceRegistration getCSourceRegistrationById(String tenantheaders, String registrationId)
			throws ResponseException, Exception {
		if (registrationId == null) {
			throw new ResponseException(ErrorType.BadRequestData);
		}

		String entityBody = validateIdAndGetBody(registrationId, tenantheaders);
		JsonNode csourceJsonBody = objectMapper.createObjectNode();
		csourceJsonBody = objectMapper.readTree(entityBody);

		return DataSerializer.getCSourceRegistration(objectMapper.writeValueAsString(csourceJsonBody));

	}

	public boolean updateCSourceRegistration(ArrayListMultimap<String, String> headers, String registrationId,
			String payload) throws ResponseException, Exception {
		MessageChannel messageChannel = producerChannels.csourceWriteChannel();
		if (registrationId == null) {
			throw new ResponseException(ErrorType.BadRequestData);
		}
		String tenantid;
		if (headers.containsKey(NGSIConstants.TENANT_HEADER)) {
			tenantid = headers.get(NGSIConstants.TENANT_HEADER).get(0);
			TenantContext.setCurrentTenant(tenantid);
		} else {
			tenantid = null;
		}

		String csourceBody = validateIdAndGetBody(registrationId, tenantid);
		JsonNode csourceJsonBody = objectMapper.createObjectNode();
		csourceJsonBody = objectMapper.readTree(csourceBody);

		CSourceRegistration prevCSourceRegistration = DataSerializer.getCSourceRegistration(csourceJsonBody.toString());
		logger.debug("Previous CSource Registration:: " + prevCSourceRegistration);

		CSourceRegistration updateCS = DataSerializer.getCSourceRegistration(payload);
		CSourceRegistration newCSourceRegistration = prevCSourceRegistration.update(updateCS);
		AppendCSourceRequest request = new AppendCSourceRequest(headers, registrationId, csourceJsonBody,
				newCSourceRegistration);

		synchronized (this) {
			TimerTask task = regId2TimerTask.get(registrationId.toString());
			if (task != null) {
				task.cancel();
			}
			this.csourceTimerTask(headers, newCSourceRegistration);
		}
		csourceSubService.checkSubscriptions(prevCSourceRegistration, newCSourceRegistration);
		this.operations.pushToKafka(messageChannel, registrationId.getBytes(),
				DataSerializer.toJson(request).getBytes());
		handleFed();
		return true;
	}

	private void handleFed() {
		new Thread() {
			public void run() {
				startupConfig.handleUpdatedTypesForFed();
			};
		}.start();
	}

	public URI registerCSource(ArrayListMultimap<String, String> headers, CSourceRegistration csourceRegistration)
			throws ResponseException, Exception {
		CSourceRequest request = new CreateCSourceRequest(csourceRegistration, headers);
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

		String tenantid;
		if (headers.containsKey(NGSIConstants.TENANT_HEADER)) {
			tenantid = headers.get(NGSIConstants.TENANT_HEADER).get(0);
			TenantContext.setCurrentTenant(tenantid);
		} else {
			tenantid = null;
		}
		if (tenantid != null) {
			String databsename = tenantAwareDataSource.findDataBaseNameByTenantId(tenantid);
			if (databsename != null) {
				this.tenantcsourceIds = csourceInfoDAO.getAllTenantIds();
				synchronized (this.tenantcsourceIds) {
					if (this.tenantcsourceIds.contains(request.getId())) {
						throw new ResponseException(ErrorType.AlreadyExists);
					}
					this.tenantcsourceIds.add(request.getId());
				}
			}

		} else {

			synchronized (this.csourceIds) {
				if (this.csourceIds.contains(request.getId())) {
					throw new ResponseException(ErrorType.AlreadyExists);
				}
				this.csourceIds.add(request.getId());
			}
		}
		// TODO: [check for valid identifier (id)]

		operations.pushToKafka(producerChannels.csourceWriteChannel(),
				request.getId().getBytes(NGSIConstants.ENCODE_FORMAT),
				DataSerializer.toJson(request).getBytes(NGSIConstants.ENCODE_FORMAT));
		this.csourceTimerTask(headers, csourceRegistration);
		if (!csourceRegistration.isInternal()) {
			csourceSubService.checkSubscriptions(csourceRegistration, TriggerReason.newlyMatching);
		}
		handleFed();
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

	public void csourceTimerTask(ArrayListMultimap<String, String> headers, CSourceRegistration csourceReg) {
		if (csourceReg.getExpires() != null) {
			TimerTask cancel = new TimerTask() {
				@Override
				public void run() {
					try {
						synchronized (this) {
							deleteCSourceRegistration(headers, csourceReg.getId().toString());
						}
					} catch (Exception e) {
						logger.error("Timer Task -> Exception while expiring residtration :: ", e);
					}
				}
			};
			regId2TimerTask.put(csourceReg.getId().toString(), cancel);
			watchDog.schedule(cancel, csourceReg.getExpires() - System.currentTimeMillis());
		}
	}

	public boolean deleteCSourceRegistration(ArrayListMultimap<String, String> headers, String registrationId)
			throws ResponseException, Exception {
		MessageChannel messageChannel = producerChannels.csourceWriteChannel();
		if (registrationId == null) {
			throw new ResponseException(ErrorType.BadRequestData);
		}

		String tenantid;
		if (headers.containsKey(NGSIConstants.TENANT_HEADER)) {
			tenantid = headers.get(NGSIConstants.TENANT_HEADER).get(0);
			TenantContext.setCurrentTenant(tenantid);
		} else {
			tenantid = null;
		}
		if (tenantid != null) {
			this.tenantcsourceIds = csourceInfoDAO.getAllTenantIds();
			synchronized (this.tenantcsourceIds) {
				if (!this.tenantcsourceIds.contains(registrationId)) {
					throw new ResponseException(ErrorType.NotFound);
				}
			}
		} else {
			synchronized (this.csourceIds) {

				if (!this.csourceIds.contains(registrationId)) {
					throw new ResponseException(ErrorType.NotFound);
				}

			}
		}

		String csourceBody = null;
		if (directDB) {
			if (tenantid != null) {
				csourceBody = this.csourceInfoDAO.getTenantEntity(registrationId);
			} else {
				csourceBody = this.csourceInfoDAO.getEntity(registrationId);
			}
		}
		CSourceRegistration csourceRegistration = DataSerializer.getCSourceRegistration(csourceBody);
		CSourceRequest request = new DeleteCSourceRequest(null, headers, registrationId);
		this.csourceSubService.checkSubscriptions(csourceRegistration, TriggerReason.noLongerMatching);
		this.operations.pushToKafka(messageChannel, registrationId.getBytes(),
				DataSerializer.toJson(request).getBytes(NGSIConstants.ENCODE_FORMAT));
		handleFed();
		return true;
		// TODO: [push to other DELETE TOPIC]
	}

	// for testing
	// @StreamListener(CSourceConsumerChannel.csourceReadChannel)
//	@KafkaListener(topics = "${csource.source.topic}", groupId = "regmanger")
//	public void handleEntityCreate(Message<?> message) {
//		String payload = new String((byte[]) message.getPayload());
//		String key = operations.getMessageKey(message);
//		logger.debug("key received ::::: " + key);
//		logger.debug("Received message: {} :::: " + payload);
//	}

	@KafkaListener(topics = "${csource.registry.topic}", groupId = "regmanger") // (CSourceConsumerChannel.contextRegistryReadChannel)
	public void handleEntityRegistration(Message<?> message) {
		executor.execute(new Thread() {
			@Override
			public void run() {
				String payload = new String((byte[]) message.getPayload());				
				String datavalue;
				JsonObject jsonObject = new JsonParser().parse(payload).getAsJsonObject();
				if (jsonObject.has("CSource")) {
					datavalue = jsonObject.get("CSource").toString();
				} else {
					datavalue = "null";
				}
				CSourceRegistration csourceRegistration = DataSerializer.getCSourceRegistration(datavalue);
				JsonObject jsonObjectpayload = new JsonParser().parse(payload).getAsJsonObject();
				ArrayListMultimap<String, String> requestheaders = ArrayListMultimap.create();
				if (jsonObjectpayload.has("headers")) {
					String headers = jsonObjectpayload.get("headers").toString();
					JsonObject jsonObjectheaders = new JsonParser().parse(headers).getAsJsonObject();
					for (Entry<String, JsonElement> entry : jsonObjectheaders.entrySet()) {
						JsonArray array = entry.getValue().getAsJsonArray();
						for (JsonElement item : array) {
							requestheaders.put(entry.getKey(), item.getAsString());
						}
					}
				}
				// objectMapper.readValue((byte[]) message.getPayload(),
				// CSourceRegistration.class);
				csourceRegistration.setInternal(true);
				try {
					registerCSource(requestheaders, csourceRegistration);
				} catch (Exception e) {
					logger.trace("Failed to register csource " + csourceRegistration.getId().toString(), e);
				}
			}
		});
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
	private boolean isValidFutureDate(Long date) {

		return System.currentTimeMillis() < date;
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

	private String validateIdAndGetBody(String registrationid, String tenantid) throws ResponseException {
		// null id check
		if (registrationid == null) {
			throw new ResponseException(ErrorType.BadRequestData);
		}
		if (tenantid != null) {
			TenantContext.setCurrentTenant(tenantid);
			this.tenantcsourceIds = csourceInfoDAO.getAllTenantIds();
			synchronized (this.tenantcsourceIds) {
				if (!this.tenantcsourceIds.contains(registrationid)) {
					throw new ResponseException(ErrorType.NotFound);
				}
			}
		} else {
			// get entity details from in-memory hashmap.
			synchronized (this.csourceIds) {
				this.csourceIds = csourceInfoDAO.getAllIds();
				if (!this.csourceIds.contains(registrationid)) {
					throw new ResponseException(ErrorType.NotFound);
				}
			}
		}
		String entityBody = null;
		if (directDB) {
			if (tenantid != null) {
				entityBody = this.csourceInfoDAO.getTenantEntity(registrationid);
			} else {
				entityBody = this.csourceInfoDAO.getEntity(registrationid);
			}
		}
		return entityBody;
	}

}
