package eu.neclab.ngsildbroker.historymanager.service;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

import com.github.jsonldjava.core.Context;
import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.requests.AppendHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.HistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.UpdateHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.results.AppendResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.UpdateResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.EntryCRUDService;
import eu.neclab.ngsildbroker.commons.ngsiqueries.ParamsResolver;
import eu.neclab.ngsildbroker.commons.querybase.BaseQueryService;
import eu.neclab.ngsildbroker.commons.storage.StorageDAO;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.historymanager.repository.HistoryDAO;

@Service
public class HistoryService extends BaseQueryService implements EntryCRUDService {

	private final static Logger logger = LoggerFactory.getLogger(HistoryService.class);

	@Autowired
	HistoryDAO historyDAO;

	@Autowired
	KafkaTemplate<String, Object> kafkaTemplate;

	@Value("${scorpio.directdb:true}")
	boolean directDB;

	@Value("${scorpio.topics.temporal}")
	private String TEMP_TOPIC;

	@Value("${scorpio.history.tokafka:false}")
	private boolean historyToKafkaEnabled;

	private ThreadPoolExecutor kafkaExecutor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.MINUTES,
			new LinkedBlockingQueue<Runnable>());
	private ArrayListMultimap<String, String> entityIds = ArrayListMultimap.create();
	
	// construct in-memory
		@PostConstruct
		private void loadStoredTemporalEntitiesDetails() throws ResponseException {
			synchronized (this.entityIds) {
				this.entityIds = historyDAO.getAllIds();
			}
			logger.trace("filling in-memory hashmap completed:");
		}
	public String createEntry(ArrayListMultimap<String, String> headers, Map<String, Object> resolved)
			throws ResponseException, Exception {
		return createTemporalEntity(headers, resolved, false);
	}

	String createTemporalEntity(ArrayListMultimap<String, String> headers, Map<String, Object> resolved,
			boolean fromEntity) throws ResponseException, Exception {
		
		CreateHistoryEntityRequest request = new CreateHistoryEntityRequest(headers, resolved, fromEntity);
		String tenantId = HttpUtils.getInternalTenant(headers);
		synchronized (this.entityIds) {
			if (this.entityIds.containsEntry(tenantId, request.getId())) {
				throw new ResponseException(ErrorType.AlreadyExists, request.getId() + " already exists");
			}
			this.entityIds.put(tenantId, request.getId());
		}
		logger.trace("creating temporal entity");
		handleRequest(request);
		return request.getId();
	}

	private void pushToKafka(BaseRequest request) throws ResponseException {
		if (historyToKafkaEnabled) {
			kafkaExecutor.execute(new Runnable() {
				@Override
				public void run() {
					kafkaTemplate.send(TEMP_TOPIC, request.getId(), new BaseRequest(request));

				}
			});
		}
	}

	private void pushToDB(HistoryEntityRequest request) throws ResponseException {
		try {
			historyDAO.storeTemporalEntity(request);
		} catch (SQLException e) {
			throw new ResponseException(ErrorType.InternalError, e.getLocalizedMessage());
		}

	}

	public boolean deleteEntry(ArrayListMultimap<String, String> headers, String entityId)
			throws ResponseException, Exception {
		return delete(headers, entityId, null, null, null);
	}

	public boolean delete(ArrayListMultimap<String, String> headers, String entityId, String attributeId,
			String instanceId, Context linkHeaders) throws ResponseException, Exception {
		
		String tenantId = HttpUtils.getInternalTenant(headers);
		synchronized (this.entityIds) {

			if (!this.entityIds.containsEntry(tenantId, entityId)) {
				throw new ResponseException(ErrorType.NotFound, entityId + " not found");
			}
			this.entityIds.remove(tenantId, entityId);
		}
		logger.debug("deleting temporal entity with id : " + entityId + "and attributeId : " + attributeId);

		String resolvedAttrId = null;
		if (attributeId != null) {
			resolvedAttrId = ParamsResolver.expandAttribute(attributeId, linkHeaders);
		}
		DeleteHistoryEntityRequest request = new DeleteHistoryEntityRequest(headers, resolvedAttrId, instanceId,
				entityId);
		handleRequest(request);
		return true;
	}
	// need to be check and change
	// endpoint "/entities/{entityId}/attrs"
	public UpdateResult appendToEntry(ArrayListMultimap<String, String> headers, String entityId,
			Map<String, Object> resolved, String[] options) throws ResponseException, Exception {
		if (!historyDAO.entityExists(entityId, HttpUtils.getTenantFromHeaders(headers))) {
			throw new ResponseException(ErrorType.NotFound, "You cannot create an attribute on a none existing entity");
		}
		AppendHistoryEntityRequest request = new AppendHistoryEntityRequest(headers, resolved, entityId);
		handleRequest(request);
		//return new AppendResult(resolved, request.getFinalPayload());
		return null;
	}

	// for endpoint "entities/{entityId}/attrs/{attrId}/{instanceId}")
	public void modifyAttribInstanceTemporalEntity(ArrayListMultimap<String, String> headers, String entityId,
			Map<String, Object> resolved, String attribId, String instanceId, Context linkHeaders)
			throws ResponseException, Exception {

		String resolvedAttrId = null;
		if (attribId != null) {
			resolvedAttrId = ParamsResolver.expandAttribute(attribId, linkHeaders);
		}

		// check if entityId + attribId + instanceid exists. if not, throw exception
		// ResourceNotFound
		QueryParams qp = new QueryParams();
		List<Map<String, String>> temp1 = new ArrayList<Map<String, String>>();
		HashMap<String, String> temp2 = new HashMap<String, String>();
		temp2.put(NGSIConstants.JSON_LD_ID, entityId);
		temp1.add(temp2);
		qp.setEntities(temp1);
		qp.setAttrs(resolvedAttrId);
		qp.setInstanceId(instanceId);
		qp.setIncludeSysAttrs(true);
		QueryResult queryResult = historyDAO.query(qp);
		List<String> entityList = queryResult.getActualDataString();
		if (entityList.size() == 0) {
			throw new ResponseException(ErrorType.NotFound, "Entity not found");
		}
		String oldEntry = "[" + String.join(",", entityList) + "]";
		UpdateHistoryEntityRequest request = new UpdateHistoryEntityRequest(headers, resolved, entityId, resolvedAttrId,
				instanceId, oldEntry);
		handleRequest(request);
	}

	@Override
	public UpdateResult updateEntry(ArrayListMultimap<String, String> headers, String entityId,
			Map<String, Object> entry) throws ResponseException, Exception {
		// History can't do this
		throw new MethodNotFoundException();
	}

	void handleRequest(HistoryEntityRequest request) throws ResponseException {
		if (directDB) {
			pushToDB(request);
		}
		pushToKafka(request);
	}

	@Override
	protected StorageDAO getQueryDAO() {
		return historyDAO;
	}

	@Override
	protected StorageDAO getCsourceDAO() {
		return null;
	}
}
