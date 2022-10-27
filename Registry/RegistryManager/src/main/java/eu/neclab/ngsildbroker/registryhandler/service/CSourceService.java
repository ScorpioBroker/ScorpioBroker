package eu.neclab.ngsildbroker.registryhandler.service;

import java.io.IOException;
import java.sql.SQLException;
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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.el.MethodNotFoundException;

import org.apache.http.HttpResponse;
import org.apache.http.MethodNotSupportedException;
import org.apache.http.client.fluent.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.requests.AppendCSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateCSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteCSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.results.CreateResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.UpdateResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.EntryCRUDService;
import eu.neclab.ngsildbroker.commons.querybase.BaseQueryService;
import eu.neclab.ngsildbroker.commons.storage.StorageDAO;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;
import eu.neclab.ngsildbroker.registryhandler.controller.RegistryController;
import eu.neclab.ngsildbroker.registryhandler.repository.CSourceDAO;

@Service
public class CSourceService extends BaseQueryService implements EntryCRUDService {

	private final static Logger logger = LoggerFactory.getLogger(RegistryController.class);

	@Autowired
	CSourceDAO csourceInfoDAO;

	Map<String, Object> myRegistryInformation;
	@Value("${scorpio.registry.autoregmode:types}")
	String AUTO_REG_MODE;

	@Value("${scorpio.registry.autorecording:active}")
	String AUTO_REG_STATUS;

	@Autowired
	KafkaTemplate<String, Object> kafkaTemplate;

	@Value("${scorpio.topics.registry}")
	String CSOURCE_TOPIC;
	private ThreadPoolExecutor kafkaExecutor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.MINUTES,
			new LinkedBlockingQueue<Runnable>());
	private ThreadPoolExecutor regStoreExecutor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.MINUTES,
			new LinkedBlockingQueue<Runnable>());
	@Value("${scorpio.directDB}")
	boolean directDB = true;

	// private ArrayListMultimap<String, String> csourceIds =
	// ArrayListMultimap.create();

	HashMap<String, TimerTask> regId2TimerTask = new HashMap<String, TimerTask>();
	Timer watchDog = new Timer(true);
	private ArrayBlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<Runnable>(50000, true);

	ThreadPoolExecutor executor = new ThreadPoolExecutor(20, 50, 600000, TimeUnit.MILLISECONDS, workQueue);

	private Table<String, Map<String, Object>, Set<String>> tenant2InformationEntry2EntityIds = HashBasedTable.create();
	private Table<String, String, Map<String, Object>> tenant2EntityId2InformationEntry = HashBasedTable.create();

	@Autowired
	private MicroServiceUtils microServiceUtils;

	@Value("${scorpio.fedbrokers:#{null}}")
	private String fedBrokers;

	@SuppressWarnings("unused")
	private void loadStoredEntitiesDetails() throws IOException, ResponseException {
		// this.csourceIds = csourceInfoDAO.getAllIds();
		if (AUTO_REG_STATUS.equals("active")) {
			Map<String, List<String>> tenant2Entity = csourceInfoDAO.getAllEntities();
			for (Entry<String, List<String>> entry : tenant2Entity.entrySet()) {
				String tenant = entry.getKey();
				List<String> entityList = entry.getValue();
				if (entityList.isEmpty()) {
					continue;
				}
				for (String entityString : entityList) {
					Map<String, Object> entity = (Map<String, Object>) JsonUtils.fromString(entityString);
					Map<String, Object> informationEntry = getInformationFromEntity(entity);
					String id = (String) entity.get(NGSIConstants.JSON_LD_ID);
					tenant2EntityId2InformationEntry.put(tenant, id, informationEntry);
					Set<String> ids = tenant2InformationEntry2EntityIds.get(tenant, informationEntry);
					if (ids == null) {
						ids = new HashSet<String>();
					}
					ids.add(id);
					tenant2InformationEntry2EntityIds.put(tenant, informationEntry, ids);
				}
				CSourceRequest regEntry = createInternalRegEntry(tenant);
				try {
					storeInternalEntry(regEntry);
				} catch (Exception e) {
					logger.error("Failed to create initial internal reg status", e);
				}
			}
		}
	}

	public List<String> getCSourceRegistrations(String tenant) throws ResponseException, IOException, Exception {
		if (directDB) {
			return csourceInfoDAO.getAllRegistrations(tenant);
		} else {
			logger.trace("getAll() ::");
			// TODO redo this completely with support of the storagemanager
			return new ArrayList<String>();
		}

	}

	public String getCSourceRegistrationById(String tenantId, String registrationId)
			throws ResponseException, Exception {
		return validateIdAndGetBody(registrationId, tenantId);
	}

	private void pushToDB(CSourceRequest request) throws SQLException, ResponseException {
		this.csourceInfoDAO.storeRegistryEntry(request);
	}

	public void csourceTimerTask(ArrayListMultimap<String, String> headers, Map<String, Object> registration) {
		Object expiresAt = registration.get(NGSIConstants.NGSI_LD_EXPIRES);
		String regId = (String) registration.get(NGSIConstants.JSON_LD_ID);
		if (expiresAt != null) {
			TimerTask cancel = new TimerTask() {
				@Override
				public void run() {
					try {
						synchronized (this) {
							deleteEntry(headers, regId);
						}
					} catch (Exception e) {
						logger.error("Timer Task -> Exception while expiring residtration :: ", e);
					}
				}
			};
			regId2TimerTask.put(regId, cancel);
			watchDog.schedule(cancel, getMillisFromDateTime(expiresAt) - System.currentTimeMillis());
		}
	}

	private String validateIdAndGetBody(String registrationid, String tenantId) throws ResponseException, Exception {
		// null id check
		if (registrationid == null) {
			throw new ResponseException(ErrorType.BadRequestData, "Invalid query for registration. No ID provided.");
		}
		String entityBody = null;
		if (directDB) {
			entityBody = this.csourceInfoDAO.getEntity(tenantId, registrationid);
		}
		return entityBody;
	}

	@Override
	public UpdateResult updateEntry(ArrayListMultimap<String, String> headers, String registrationId,
			Map<String, Object> entry) throws ResponseException, Exception {
		throw new MethodNotFoundException("not supported in registry");
	}

	public UpdateResult updateEntry(ArrayListMultimap<String, String> headers, String registrationId,
			Map<String, Object> entry, BatchInfo batchInfo) throws ResponseException, Exception {
		throw new MethodNotFoundException("not supported in registry");
	}

	// need to be check and change
	@Override
	public UpdateResult appendToEntry(ArrayListMultimap<String, String> headers, String registrationId,
			Map<String, Object> entry, String[] options, BatchInfo batchInfo) throws ResponseException, Exception {
		String tenantId = HttpUtils.getInternalTenant(headers);
		Map<String, Object> originalRegistration = validateIdAndGetBodyAsMap(registrationId, tenantId);
		AppendCSourceRequest request = new AppendCSourceRequest(headers, registrationId, originalRegistration, entry,
				options);
		TimerTask task = regId2TimerTask.get(registrationId);
		if (task != null) {
			task.cancel();
		}
		this.csourceTimerTask(headers, request.getFinalPayload());
		pushToDB(request);
		request.setBatchInfo(batchInfo);
		sendToKafka(request);
		return request.getUpdateResult();
	}

	public UpdateResult appendToEntry(ArrayListMultimap<String, String> headers, String registrationId,
			Map<String, Object> entry, String[] options) throws ResponseException, Exception {
		return appendToEntry(headers, registrationId, entry, options, new BatchInfo(-1, -1));
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> validateIdAndGetBodyAsMap(String registrationId, String tenantId) throws Exception {
		return (Map<String, Object>) JsonUtils.fromString(validateIdAndGetBody(registrationId, tenantId));
	}

	@Override
	public CreateResult createEntry(ArrayListMultimap<String, String> headers, Map<String, Object> resolved,
			BatchInfo batchInfo) throws ResponseException, Exception {
		String id;
		Object idObj = resolved.get(NGSIConstants.JSON_LD_ID);
		if (idObj == null) {
			id = EntityTools.generateUniqueRegId(resolved);
			resolved.put(NGSIConstants.JSON_LD_ID, id);
		} else {
			id = (String) idObj;
		}
		CSourceRequest request = new CreateCSourceRequest(resolved, headers, id);
		request.setBatchInfo(batchInfo);
		/*
		 * String tenantId = HttpUtils.getInternalTenant(headers); if
		 * (this.csourceIds.containsEntry(tenantId, request.getId())) { throw new
		 * ResponseException(ErrorType.AlreadyExists, "CSource already exists"); }
		 * this.csourceIds.put(tenantId, request.getId());
		 */
		pushToDB(request);
		sendToKafka(request);
		return new CreateResult(request.getId(), true);

	}

	public CreateResult createEntry(ArrayListMultimap<String, String> headers, Map<String, Object> resolved)
			throws ResponseException, Exception {
		return createEntry(headers, resolved, new BatchInfo(-1, -1));
	}

	private void sendToKafka(BaseRequest request) {
		kafkaExecutor.execute(new Runnable() {
			@Override
			public void run() {
				kafkaTemplate.send(CSOURCE_TOPIC, request.getId(), new BaseRequest(request));

			}
		});
	}

	public boolean deleteEntry(ArrayListMultimap<String, String> headers, String registrationId)
			throws ResponseException, Exception {
		return deleteEntry(headers, registrationId, new BatchInfo(-1, -1));
	}

	@Override
	public boolean deleteEntry(ArrayListMultimap<String, String> headers, String registrationId, BatchInfo batchInfo)
			throws ResponseException, Exception {
		if (registrationId == null) {
			throw new ResponseException(ErrorType.BadRequestData, "Invalid delete for registration. No ID provided.");
		}

		String tenantId = HttpUtils.getInternalTenant(headers);

		// if (!this.csourceIds.containsEntry(tenantId, registrationId)) {
		// throw new ResponseException(ErrorType.NotFound, registrationId + " not
		// found.");
		// }

		Map<String, Object> registration = validateIdAndGetBodyAsMap(registrationId, tenantId);
		CSourceRequest requestForSub = new DeleteCSourceRequest(registration, headers, registrationId);
		requestForSub.setBatchInfo(batchInfo);
		sendToKafka(requestForSub);
		CSourceRequest request = new DeleteCSourceRequest(null, headers, registrationId);
		pushToDB(request);
		// this.csourceIds.remove(tenantId, registrationId);
		return true;

	}

	private long getMillisFromDateTime(Object expiresAt) {
		@SuppressWarnings("unchecked")
		String value = (String) ((List<Map<String, Object>>) expiresAt).get(0).get(NGSIConstants.JSON_LD_VALUE);
		try {
			return SerializationTools.date2Long(value);
		} catch (Exception e) {
			throw new AssertionError("In invalid date time came pass the payload checker");
		}
	}

	public QueryResult query(QueryParams qp) throws ResponseException {
		return csourceInfoDAO.query(qp);
	}

	@Override
	protected StorageDAO getQueryDAO() {
		return csourceInfoDAO;
	}

	@Override
	protected StorageDAO getCsourceDAO() {
		// intentional null!!!
		return null;
	}

	public void handleEntityDelete(BaseRequest message) {
		String id = message.getId();
		String tenant = message.getTenant();
		Map<String, Object> informationEntry = tenant2EntityId2InformationEntry.remove(tenant, id);
		Set<String> ids = tenant2InformationEntry2EntityIds.get(tenant, informationEntry);
		if (ids == null) {
			return;
		}
		ids.remove(id);
		if (ids.isEmpty()) {
			tenant2InformationEntry2EntityIds.remove(tenant, informationEntry);
			try {
				CSourceRequest regEntry = createInternalRegEntry(tenant);
				if (regEntry.getFinalPayload() == null) {
					deleteEntry(regEntry.getHeaders(), regEntry.getId());
				} else {
					storeInternalEntry(regEntry);
				}
			} catch (Exception e) {
				logger.error("Failed to store internal registry entry", e);
			}
		} else {
			tenant2InformationEntry2EntityIds.put(tenant, informationEntry, ids);
		}
	}

	public void handleEntityCreateOrUpdate(BaseRequest message) {
		Map<String, Object> informationEntry = getInformationFromEntity(message.getFinalPayload());
		checkInformationEntry(informationEntry, message.getId(), message.getTenant());
	}

	private void checkInformationEntry(Map<String, Object> informationEntry, String id, String tenant) {
		if (informationEntry == null) {
			return;
		}
		boolean update = true;
		Set<String> ids = tenant2InformationEntry2EntityIds.get(tenant, informationEntry);
		if (ids == null) {
			ids = new HashSet<String>();
		} else {
			update = false;
		}
		ids.add(id);
		tenant2InformationEntry2EntityIds.put(tenant, informationEntry, ids);
		tenant2EntityId2InformationEntry.put(tenant, id, informationEntry);
		if (update) {
			try {
				CSourceRequest regEntry = createInternalRegEntry(tenant);
				if (regEntry.getFinalPayload() == null) {
					deleteEntry(regEntry.getHeaders(), regEntry.getId());
				} else {
					storeInternalEntry(regEntry);
				}
			} catch (Exception e) {
				logger.error("Failed to store internal registry entry", e);
			}
		}

	}

	private CSourceRequest createInternalRegEntry(String tenant) {
		String id = AppConstants.INTERNAL_REGISTRATION_ID;
		ArrayListMultimap<String, String> headers = ArrayListMultimap.create();
		if (!tenant.equals(AppConstants.INTERNAL_NULL_KEY)) {
			id += ":" + tenant;
			headers.put(NGSIConstants.TENANT_HEADER_FOR_INTERNAL_CHECK, tenant);
		}
		Map<String, Object> resolved = new HashMap<String, Object>();
		resolved.put(NGSIConstants.JSON_LD_ID, id);
		ArrayList<Object> tmp = new ArrayList<Object>();
		tmp.add(NGSIConstants.NGSI_LD_CSOURCE_REGISTRATION);
		resolved.put(NGSIConstants.JSON_LD_TYPE, tmp);
		tmp = new ArrayList<Object>();
		HashMap<String, Object> tmp2 = new HashMap<String, Object>();
		tmp2.put(NGSIConstants.JSON_LD_VALUE, microServiceUtils.getGatewayURL().toString());
		tmp.add(tmp2);
		resolved.put(NGSIConstants.NGSI_LD_ENDPOINT, tmp);
		Set<Map<String, Object>> informationEntries = tenant2InformationEntry2EntityIds.row(tenant).keySet();

		tmp = new ArrayList<Object>();
		for (Map<String, Object> entry : informationEntries) {
			tmp.add(entry);
		}
		try {
			if (tmp.isEmpty()) {
				return new DeleteCSourceRequest(null, headers, id);
			}
			resolved.put(NGSIConstants.NGSI_LD_INFORMATION, tmp);

			return new CreateCSourceRequest(resolved, headers, id);
		} catch (ResponseException e) {
			logger.error("failed to create internal registry entry", e);
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> getInformationFromEntity(Map<String, Object> entity) {
		HashMap<String, Object> result = new HashMap<String, Object>();
		Map<String, Object> entities = new HashMap<String, Object>();
		ArrayList<Map<String, Object>> propertyNames = new ArrayList<Map<String, Object>>();
		ArrayList<Map<String, Object>> relationshipNames = new ArrayList<Map<String, Object>>();
		for (Entry<String, Object> entry : entity.entrySet()) {
			String key = entry.getKey();
			Object value = entry.getValue();
			if (key.equals(NGSIConstants.JSON_LD_ID)) {
				if (AUTO_REG_MODE.contains("ids")) {
					entities.put(NGSIConstants.JSON_LD_ID, value);
				}
				continue;
			}
			if (key.equals(NGSIConstants.JSON_LD_TYPE)) {
				if (AUTO_REG_MODE.contains("types")) {
					entities.put(NGSIConstants.JSON_LD_TYPE, entity.get(NGSIConstants.JSON_LD_TYPE));
				}
				continue;
			}
			if (AUTO_REG_MODE.contains("attributes")) {
				if (value instanceof List) {
					Object listValue = ((List<Object>) value).get(0);
					if (listValue instanceof Map) {
						Map<String, Object> mapValue = (Map<String, Object>) listValue;
						Object type = mapValue.get(NGSIConstants.JSON_LD_TYPE);
						if (type != null) {
							String typeString;
							if (type instanceof List) {
								typeString = ((List<String>) type).get(0);
							} else if (type instanceof String) {
								typeString = (String) type;
							} else {
								continue;
							}

							HashMap<String, Object> tmp = new HashMap<String, Object>();
							tmp.put(NGSIConstants.JSON_LD_ID, entry.getKey());
							switch (typeString) {
								case NGSIConstants.NGSI_LD_GEOPROPERTY:
								case NGSIConstants.NGSI_LD_PROPERTY:
									propertyNames.add(tmp);
									break;
								case NGSIConstants.NGSI_LD_RELATIONSHIP:
									relationshipNames.add(tmp);
									break;
								default:
									continue;
							}
						}
					}
				}
			}
		}
		if (!entities.isEmpty()) {
			ArrayList<Map<String, Object>> tmp = new ArrayList<Map<String, Object>>();
			tmp.add(entities);
			result.put(NGSIConstants.NGSI_LD_ENTITIES, tmp);
		}
		if (!propertyNames.isEmpty()) {
			result.put(NGSIConstants.NGSI_LD_PROPERTIES, propertyNames);
		}
		if (!relationshipNames.isEmpty()) {
			result.put(NGSIConstants.NGSI_LD_PROPERTIES, propertyNames);
		}
		if (!result.isEmpty()) {
			return result;
		}
		return null;
	}

	private void storeInternalEntry(CSourceRequest regEntry) {
		boolean failed = false;
		try {
			appendToEntry(regEntry.getHeaders(), regEntry.getId(), regEntry.getFinalPayload(), null);
		} catch (ResponseException e) {
			try {
				createEntry(regEntry.getHeaders(), regEntry.getFinalPayload());
			} catch (Exception e1) {
				failed = true;
				logger.error("Failed to store internal regentry", e1);
			}
		} catch (Exception e) {
			failed = true;
			logger.error("Failed to store internal regentry", e);
		}
		if (!failed && fedBrokers != null && !fedBrokers.isBlank()) {
			regStoreExecutor.execute(new Runnable() {

				@Override
				public void run() {
					int retry = 5;
					for (String fedBroker : fedBrokers.split(",")) {
						while (true) {
							try {

								if (!fedBroker.endsWith("/")) {
									fedBroker += "/";
								}
								HashMap<String, Object> copyToSend = Maps.newHashMap(regEntry.getFinalPayload());
								String csourceId = microServiceUtils.getGatewayURL().toString();
								copyToSend.put(NGSIConstants.JSON_LD_ID, csourceId);

								HttpResponse resp = Request
										.Patch(fedBroker + "ngsi-ld/v1/csourceRegistrations/" + csourceId)
										.addHeader("Content-Type", "application/ld+json")
										.bodyByteArray(JsonUtils
												.toPrettyString(JsonLdProcessor.compact(copyToSend, null, opts))
												.getBytes())
										.execute().returnResponse();
								int returnCode = resp.getStatusLine().getStatusCode();
								if (returnCode == ErrorType.NotFound.getCode()) {
									resp = Request.Post(fedBroker + "ngsi-ld/v1/csourceRegistrations/")
											.addHeader("Content-Type", "application/ld+json")
											.bodyByteArray(JsonUtils
													.toPrettyString(JsonLdProcessor.compact(copyToSend, null, opts))
													.getBytes())
											.execute().returnResponse();
									returnCode = resp.getStatusLine().getStatusCode();
								}
								if (resp.getStatusLine().getStatusCode() >= 200
										&& resp.getStatusLine().getStatusCode() < 300) {
									return;
								}

							} catch (JsonLdError | IOException | ResponseException e) {
								logger.error("Failed to register with fed broker", e);
							}
							retry--;
							if (retry <= 0) {
								return;
							}
						}
					}

				}
			});

		}

	}

	@Override
	public void sendFail(BatchInfo batchInfo) {
		throw new MethodNotFoundException("Registry doesn't do batches");

	}

}
