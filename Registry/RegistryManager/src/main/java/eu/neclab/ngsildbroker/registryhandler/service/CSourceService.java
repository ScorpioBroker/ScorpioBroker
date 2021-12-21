package eu.neclab.ngsildbroker.registryhandler.service;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
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
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ArrayListMultimap;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import eu.neclab.ngsildbroker.commons.constants.DBConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.AppendCSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.CSourceRegistration;
import eu.neclab.ngsildbroker.commons.datatypes.CSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.CreateCSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.DeleteCSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.QueryResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.enums.TriggerReason;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.serialization.DataSerializer;
import eu.neclab.ngsildbroker.commons.storage.StorageWriterDAO;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
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
	ObjectMapper objectMapper;

	@Autowired
	StartupConfig startupConfig;

	@Autowired
	CSourceDAO csourceDAO;

	@Autowired
	CSourceInfoDAO csourceInfoDAO;

	@Autowired
	CSourceSubscriptionService csourceSubService;

	@Autowired
	KafkaTemplate<String, String> kafkaTemplate;

	@Value("${csource.source.topic}")
	String CSOURCE_TOPIC;

	@Value("${csource.directdb:true}")
	boolean directDB = true;

	@Autowired
	@Qualifier("csdao")
	StorageWriterDAO storageWriterDao;

	private ArrayListMultimap<String, String> csourceIds = ArrayListMultimap.create();

	HashMap<String, TimerTask> regId2TimerTask = new HashMap<String, TimerTask>();
	Timer watchDog = new Timer(true);
	private ArrayBlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<Runnable>(50000, true);

	ThreadPoolExecutor executor = new ThreadPoolExecutor(20, 50, 600000, TimeUnit.MILLISECONDS, workQueue);

	@PostConstruct
	private void loadStoredEntitiesDetails() throws IOException, ResponseException {
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
			// TODO redo this completely with support of the storagemanager
			/*
			 * Map<String, byte[]> records = operations.pullFromKafka(this.CSOURCE_TOPIC);
			 * Map<String, JsonNode> entityMap = new HashMap<String, JsonNode>(); JsonNode
			 * entityJsonBody = objectMapper.createObjectNode(); byte[] result = null;
			 * String key = null;
			 * 
			 * for (String recordKey : records.keySet()) { result = records.get(recordKey);
			 * key = recordKey; entityJsonBody = objectMapper.readTree(result); if
			 * (!entityJsonBody.isNull()) entityMap.put(key, entityJsonBody); }
			 */
			return new ArrayList<JsonNode>();
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
			throw new ResponseException(ErrorType.BadRequestData, "Invalid query for registration. No ID provided.");
		}

		String entityBody = validateIdAndGetBody(registrationId, tenantheaders);
		JsonNode csourceJsonBody = objectMapper.createObjectNode();
		csourceJsonBody = objectMapper.readTree(entityBody);

		return DataSerializer.getCSourceRegistration(objectMapper.writeValueAsString(csourceJsonBody));

	}

	public boolean updateCSourceRegistration(ArrayListMultimap<String, String> headers, String registrationId,
			String payload) throws ResponseException, Exception {
		if (registrationId == null) {
			throw new ResponseException(ErrorType.BadRequestData, "Invalid update for registration. No ID provided.");
		}
		String tenantid;
		if (headers.containsKey(NGSIConstants.TENANT_HEADER)) {
			tenantid = headers.get(NGSIConstants.TENANT_HEADER).get(0);
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
		csourceSubService.checkSubscriptions(new CreateCSourceRequest(prevCSourceRegistration, headers),
				new CreateCSourceRequest(newCSourceRegistration, headers));

		// kafkaTemplate.send(CSOURCE_TOPIC, registrationId,
		// DataSerializer.toJson(request));
		// handleFed();
		pushToDB(request);
		new Thread() {
			public void run() {
				kafkaTemplate.send(CSOURCE_TOPIC, request.getId(), DataSerializer.toJson(request));
			};
		}.start();
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
		URI idUri = csourceRegistration.getId();
		if (idUri == null) {
			idUri = generateUniqueRegId(csourceRegistration);
			csourceRegistration.setId(idUri);

		}

		if (csourceRegistration.getType() == null) {
			logger.error("Invalid type!");
			throw new ResponseException(ErrorType.BadRequestData, "Invalid type");
		}
		if (!isValidURL(csourceRegistration.getEndpoint().toString())) {
			logger.error("Invalid endpoint URL!");
			throw new ResponseException(ErrorType.BadRequestData, "Invalid endpoint URL!");
		}
		if (csourceRegistration.getInformation() == null) {
			logger.error("Information is empty!");
			throw new ResponseException(ErrorType.BadRequestData, "Information is empty!");
		}
		if (csourceRegistration.getExpiresAt() != null && !isValidFutureDate(csourceRegistration.getExpiresAt())) {
			logger.error("Invalid expire date!");
			throw new ResponseException(ErrorType.BadRequestData, "Invalid expire date!");
		}

		String tenantId = HttpUtils.getInternalTenant(headers);
		synchronized (this.csourceIds) {
			if (this.csourceIds.containsEntry(tenantId, request.getId())) {
				throw new ResponseException(ErrorType.AlreadyExists, "CSource already exists");
			}
			this.csourceIds.put(tenantId, request.getId());
		}

		// TODO: [check for valid identifier (id)]
//		kafkaTemplate.send(CSOURCE_TOPIC, request.getId(), DataSerializer.toJson(request));
//		this.csourceTimerTask(headers, csourceRegistration);
//		if (!csourceRegistration.isInternal()) {
//			csourceSubService.checkSubscriptions(request, TriggerReason.newlyMatching);
//		}
//		handleFed();
		pushToDB(request);
		new Thread() {
			public void run() {
				kafkaTemplate.send(CSOURCE_TOPIC, request.getId(), DataSerializer.toJson(request));
			};
		}.start();
		return idUri;
	}

	private void pushToDB(CSourceRequest request) throws SQLException {
		boolean success = false;

		String datavalue;
		JsonObject jsonObject = new JsonParser().parse(DataSerializer.toJson(request)).getAsJsonObject();
		if (jsonObject.has("CSource")) {
			datavalue = jsonObject.get("CSource").toString();
		} else {
			datavalue = "null";
		}
		String header = jsonObject.get("headers").toString();
		JsonObject jsonObjectheader = new JsonParser().parse(header).getAsJsonObject();
		String headervalue;
		if (jsonObjectheader.has(NGSIConstants.TENANT_HEADER)) {
			headervalue = jsonObjectheader.get(NGSIConstants.TENANT_HEADER).getAsString();
			String databasename = "ngb" + headervalue;
			if (datavalue != null) {
				storageWriterDao.storeTenantdata(DBConstants.DBTABLE_CSOURCE_TENANT, DBConstants.DBCOLUMN_DATA_TENANT,
						headervalue, databasename);
			}
		} else {
			headervalue = null;
		}
		while (!success) {
			try {
				// logger.debug("Received message: " + request.getWithSysAttrs());
				logger.trace("Writing data...");
				if (storageWriterDao != null && storageWriterDao.store(DBConstants.DBTABLE_CSOURCE,
						DBConstants.DBCOLUMN_DATA, request.getId(), datavalue, headervalue)) {
					logger.trace("Writing is complete");
				}
				success = true;
			} catch (SQLTransientConnectionException e) {
				logger.warn("SQL Exception attempting retry");
				Random random = new Random();
				int randomNumber = random.nextInt(4000) + 500;
				try {
					Thread.sleep(randomNumber);
				} catch (InterruptedException e1) {

				}
			}
		}
	}

	private URI generateUniqueRegId(CSourceRegistration csourceRegistration) {

		try {

			String key = "urn:ngsi-ld:csourceregistration:"
					+ UUID.fromString(csourceRegistration.hashCode() + "").toString();
			return new URI(key);
		} catch (URISyntaxException e) {
			// Left empty intentionally should never happen
			throw new AssertionError();
		}
	}

	public void csourceTimerTask(ArrayListMultimap<String, String> headers, CSourceRegistration csourceReg) {
		if (csourceReg.getExpiresAt() != null) {
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
			watchDog.schedule(cancel, csourceReg.getExpiresAt() - System.currentTimeMillis());
		}
	}

	public boolean deleteCSourceRegistration(ArrayListMultimap<String, String> headers, String registrationId)
			throws ResponseException, Exception {
		if (registrationId == null) {
			throw new ResponseException(ErrorType.BadRequestData, "Invalid delete for registration. No ID provided.");
		}

		String tenantId = HttpUtils.getInternalTenant(headers);
		
		synchronized (this.csourceIds) {
			if (!this.csourceIds.containsEntry(tenantId, registrationId)) {
				throw new ResponseException(ErrorType.NotFound, registrationId + " not found.");
			} else {
				this.csourceIds.remove(tenantId, registrationId);

			}
		}

		/*
		 * String csourceBody = null; if (directDB) { csourceBody =
		 * this.csourceInfoDAO.getEntity(tenantId, registrationId);
		 * 
		 * }
		 */
		// CSourceRegistration csourceRegistration =
		// DataSerializer.getCSourceRegistration(csourceBody);
		
		CSourceRegistration registration = this.getCSourceRegistrationById(tenantId, registrationId);
		CSourceRequest requestForSub = new DeleteCSourceRequest(registration, headers, registrationId);
		this.csourceSubService.checkSubscriptions(requestForSub, TriggerReason.noLongerMatching);
		CSourceRequest request = new DeleteCSourceRequest(null, headers, registrationId);
		pushToDB(request);
		return true;
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

	@KafkaListener(topics = "${csource.registry.topic}") // (CSourceConsumerChannel.contextRegistryReadChannel)
	public void handleEntityRegistration(@Payload String message,
			@Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key) {
		executor.execute(new Thread() {
			@Override
			public void run() {

				if (message.equals("null")) {
					// TODO Delete registration
				} else {
					String datavalue;
					JsonObject jsonObject = new JsonParser().parse(message).getAsJsonObject();
					datavalue = jsonObject.get("CSource").toString();
					CSourceRegistration csourceRegistration = DataSerializer.getCSourceRegistration(datavalue);
					JsonObject jsonObjectpayload = new JsonParser().parse(message).getAsJsonObject();
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

	@KafkaListener(topics = "${csource.query.topic}")
	public void handleContextQuery(@Payload String message, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key)
			throws Exception {
		logger.trace("handleContextQuery() :: started");
		logger.debug("Received message: " + message);
		String resultPayload = "";
		try {
			QueryParams qp = DataSerializer.getQueryParams(message);
			QueryResult csourceList = csourceDAO.queryExternalCsources(qp);
			resultPayload = csourceDAO.getListAsJsonArray(csourceList.getActualDataString());
		} catch (Exception e) {
			e.printStackTrace();
		}
		logger.trace("Pushing result to Kafka... ");
		logger.trace("handleContextQuery() :: completed");
		kafkaTemplate.send("${csource.query.result.topic}", key, resultPayload);
	}

	private String validateIdAndGetBody(String registrationid, String tenantId) throws ResponseException {
		// null id check
		if (registrationid == null) {
			throw new ResponseException(ErrorType.BadRequestData, "Invalid query for registration. No ID provided.");
		}

		synchronized (this.csourceIds) {
			if (tenantId != null) {
				if (!this.csourceIds.containsKey(tenantId)) {
					throw new ResponseException(ErrorType.TenantNotFound, "Tenant not found");
				}
				if (!this.csourceIds.containsValue(registrationid)) {
					throw new ResponseException(ErrorType.NotFound, registrationid + " not found");
				}
			} else {
				if (!this.csourceIds.containsValue(registrationid)) {
					throw new ResponseException(ErrorType.NotFound, registrationid + " not found");
				}
			}
		}
		String entityBody = null;
		if (directDB) {
			entityBody = this.csourceInfoDAO.getEntity(tenantId, registrationid);
		}
		return entityBody;
	}

}
