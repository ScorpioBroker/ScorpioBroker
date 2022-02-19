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

import javax.annotation.PostConstruct;
import javax.el.MethodNotFoundException;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.requests.AppendCSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateCSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteCSourceRequest;
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
import eu.neclab.ngsildbroker.registryhandler.repository.RegistryCSourceDAO;
import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.MutinyEmitter;

@Singleton
public class CSourceService extends BaseQueryService implements EntryCRUDService {

	private final static Logger logger = LoggerFactory.getLogger(RegistryController.class);

	@Inject
	RegistryCSourceDAO csourceInfoDAO;

	Map<String, Object> myRegistryInformation;
	@ConfigProperty(name = "scorpio.registry.autoregmode", defaultValue = "types")
	String AUTO_REG_MODE;

	@ConfigProperty(name = "scorpio.registry.autorecording", defaultValue = "active")
	String AUTO_REG_STATUS;

	@Inject
	@Channel(AppConstants.REGISTRY_CHANNEL)
	MutinyEmitter<BaseRequest> kafkaSender;

	@ConfigProperty(name = "scorpio.topics.registry")
	String CSOURCE_TOPIC;

	private ThreadPoolExecutor kafkaExecutor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.MINUTES,
			new LinkedBlockingQueue<Runnable>());
	@ConfigProperty(name = "scorpio.directDB")
	boolean directDB = true;

	private ArrayListMultimap<String, String> csourceIds = ArrayListMultimap.create();

	HashMap<String, TimerTask> regId2TimerTask = new HashMap<String, TimerTask>();
	Timer watchDog = new Timer(true);
	private ArrayBlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<Runnable>(50000, true);

	ThreadPoolExecutor executor = new ThreadPoolExecutor(20, 50, 600000, TimeUnit.MILLISECONDS, workQueue);

	private Table<String, Map<String, Object>, Set<String>> tenant2InformationEntry2EntityIds = HashBasedTable.create();
	private Table<String, String, Map<String, Object>> tenant2EntityId2InformationEntry = HashBasedTable.create();

	@Inject
	MicroServiceUtils microServiceUtils;

	@PostConstruct
	void loadStoredEntitiesDetails() throws IOException, ResponseException {
		synchronized (this.csourceIds) {
			this.csourceIds = csourceInfoDAO.getAllIds();
		}
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
					upsert(regEntry);
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

	private void pushToDB(CSourceRequest request) throws SQLException {
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

	@Override
	public UpdateResult updateEntry(ArrayListMultimap<String, String> headers, String registrationId,
			Map<String, Object> entry) throws ResponseException, Exception {
		throw new MethodNotFoundException("not supported in registry");
	}

	// need to be check and change
	@Override
	public UpdateResult appendToEntry(ArrayListMultimap<String, String> headers, String registrationId,
			Map<String, Object> entry, String[] options) throws ResponseException, Exception {
		String tenantId = HttpUtils.getInternalTenant(headers);
		Map<String, Object> originalRegistration = validateIdAndGetBodyAsMap(registrationId, tenantId);
		AppendCSourceRequest request = new AppendCSourceRequest(headers, registrationId, originalRegistration, entry,
				options);
		synchronized (this) {
			TimerTask task = regId2TimerTask.get(registrationId);
			if (task != null) {
				task.cancel();
			}
			this.csourceTimerTask(headers, request.getFinalPayload());
		}
		pushToDB(request);
		sendToKafka(request);
		return request.getUpdateResult();
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> validateIdAndGetBodyAsMap(String registrationId, String tenantId) throws Exception {
		return (Map<String, Object>) JsonUtils.fromString(validateIdAndGetBody(registrationId, tenantId));
	}

	@Override
	public Uni<String> createEntry(ArrayListMultimap<String, String> headers, Map<String, Object> resolved) {
		String id;
		Object idObj = resolved.get(NGSIConstants.JSON_LD_ID);
		if (idObj == null) {
			id = EntityTools.generateUniqueRegId(resolved);
			resolved.put(NGSIConstants.JSON_LD_ID, id);
		} else {
			id = (String) idObj;
		}
		String tenantId = HttpUtils.getInternalTenant(headers);
		synchronized (this.csourceIds) {
			if (this.csourceIds.containsEntry(tenantId, id)) {
				return Uni.createFrom()
						.failure(new ResponseException(ErrorType.AlreadyExists, "CSource already exists"));
			}
		}
		CSourceRequest request;
		try {
			request = new CreateCSourceRequest(resolved, headers, id);
		} catch (ResponseException e) {
			return Uni.createFrom().failure(e);
		}
		return Uni.combine().all()
				.unis(csourceInfoDAO.storeRegistryEntry(request), kafkaSender.send(new BaseRequest(request)))
				.combinedWith((t, u) -> {
					synchronized (this.csourceIds) {
						this.csourceIds.put(tenantId, request.getId());
					}
					return request.getId();
				});
	}

	private void sendToKafka(BaseRequest request) {
		kafkaSender.send(new BaseRequest(request));
	}

	@Override
	public boolean deleteEntry(ArrayListMultimap<String, String> headers, String registrationId)
			throws ResponseException, Exception {
		if (registrationId == null) {
			throw new ResponseException(ErrorType.BadRequestData, "Invalid delete for registration. No ID provided.");
		}

		String tenantId = HttpUtils.getInternalTenant(headers);

		synchronized (this.csourceIds) {
			if (!this.csourceIds.containsEntry(tenantId, registrationId)) {
				throw new ResponseException(ErrorType.NotFound, registrationId + " not found.");
			}
		}

		Map<String, Object> registration = validateIdAndGetBodyAsMap(registrationId, tenantId);
		CSourceRequest requestForSub = new DeleteCSourceRequest(registration, headers, registrationId);
		sendToKafka(requestForSub);
		CSourceRequest request = new DeleteCSourceRequest(null, headers, registrationId);
		pushToDB(request);
		this.csourceIds.remove(tenantId, registrationId);
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

	public Uni<QueryResult> query(QueryParams qp) {
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
		ids.remove(id);
		if (ids.isEmpty()) {
			tenant2InformationEntry2EntityIds.remove(tenant, informationEntry);
			try {
				CSourceRequest regEntry = createInternalRegEntry(tenant);
				if (regEntry.getFinalPayload() == null) {
					deleteEntry(regEntry.getHeaders(), regEntry.getId());
				} else {
					upsert(regEntry);
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
					upsert(regEntry);
				}
			} catch (Exception e) {
				logger.error("Failed to store internal registry entry 1s", e);
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

	private void upsert(CSourceRequest regEntry) throws ResponseException, Exception {
		if (csourceIds.containsEntry(regEntry.getTenant(), regEntry.getId())) {
			appendToEntry(regEntry.getHeaders(), regEntry.getId(), regEntry.getFinalPayload(), null);
		} else {
			createEntry(regEntry.getHeaders(), regEntry.getFinalPayload());
		}

	}

}
