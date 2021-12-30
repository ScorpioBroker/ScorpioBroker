package eu.neclab.ngsildbroker.registryhandler.service;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.el.MethodNotFoundException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.requests.AppendCSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateCSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteCSourceRequest;
import eu.neclab.ngsildbroker.commons.datatypes.results.AppendResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.UpdateResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.EntryCRUDService;
import eu.neclab.ngsildbroker.commons.storage.StorageDAO;
import eu.neclab.ngsildbroker.commons.subscriptionbase.querybase.BaseQueryService;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;
import eu.neclab.ngsildbroker.registryhandler.controller.RegistryController;
import eu.neclab.ngsildbroker.registryhandler.repository.CSourceDAO;

@Service
public class CSourceService extends BaseQueryService implements EntryCRUDService {

	private final static Logger logger = LoggerFactory.getLogger(RegistryController.class);

	@Autowired
	CSourceDAO csourceInfoDAO;

	@Autowired
	KafkaTemplate<String, Object> kafkaTemplate;



	@Value("${scorpio.topics.registry}")
	String CSOURCE_TOPIC;
	private ThreadPoolExecutor kafkaExecutor = new ThreadPoolExecutor(1,1,1,TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>());
	@Value("${scorpio.directDB}")
	boolean directDB = true;

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

	@Override
	public AppendResult appendToEntry(ArrayListMultimap<String, String> headers, String registrationId,
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
		return new AppendResult(entry, request.getFinalPayload());
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> validateIdAndGetBodyAsMap(String registrationId, String tenantId) throws Exception {
		return (Map<String, Object>) JsonUtils.fromString(validateIdAndGetBody(registrationId, tenantId));
	}

	@Override
	public String createEntry(ArrayListMultimap<String, String> headers, Map<String, Object> resolved)
			throws ResponseException, Exception {
		String id;
		Object idObj = resolved.get(NGSIConstants.JSON_LD_ID);
		if (idObj == null) {
			id = EntityTools.generateUniqueRegId(resolved);
			resolved.put(NGSIConstants.JSON_LD_ID, id);
		} else {
			id = (String) idObj;
		}
		CSourceRequest request = new CreateCSourceRequest(resolved, headers, id);
		String tenantId = HttpUtils.getInternalTenant(headers);
		synchronized (this.csourceIds) {
			if (this.csourceIds.containsEntry(tenantId, request.getId())) {
				throw new ResponseException(ErrorType.AlreadyExists, "CSource already exists");
			}
			this.csourceIds.put(tenantId, request.getId());
		}
		pushToDB(request);
		sendToKafka(request);
		return request.getId();

	}

	private void sendToKafka(BaseRequest request) {
		kafkaExecutor.execute(new Runnable() {
			@Override
			public void run() {
				kafkaTemplate.send(CSOURCE_TOPIC, request.getId(), new BaseRequest(request));
				
			}
		});
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

	public QueryResult query(QueryParams qp) throws ResponseException {
		return csourceInfoDAO.query(qp);
	}

	@Override
	protected StorageDAO getQueryDAO() {
		return csourceInfoDAO;
	}

	@Override
	protected StorageDAO getCsourceDAO() {
		return null;
	}

}
