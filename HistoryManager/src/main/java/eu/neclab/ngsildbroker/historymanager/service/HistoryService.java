package eu.neclab.ngsildbroker.historymanager.service;

import java.io.IOException;
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
import javax.inject.Inject;
import javax.inject.Singleton;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.ArrayListMultimap;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.requests.AppendHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.EntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.HistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.UpdateHistoryEntityRequest;
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
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.groups.UniAndGroup2;
import io.smallrye.reactive.messaging.MutinyEmitter;

@Singleton
public class HistoryService extends BaseQueryService implements EntryCRUDService {

	private final static Logger logger = LoggerFactory.getLogger(HistoryService.class);

	@Inject
	HistoryDAO historyDAO;

//	@Inject
//	KafkaTemplate<String, Object> kafkaTemplate;

	@ConfigProperty(name = "scorpio.directDB", defaultValue = "true")
	boolean directDB;

	@ConfigProperty(name = "scorpio.topics.temporal")
	private String TEMP_TOPIC;

	@ConfigProperty(name = "scorpio.history.tokafka", defaultValue = "false")
	private boolean historyToKafkaEnabled;

	@Inject
	@Channel(AppConstants.ENTITY_CHANNEL)
	MutinyEmitter<BaseRequest> kafkaSenderInterface;

	// private ThreadPoolExecutor kafkaExecutor = new ThreadPoolExecutor(1, 1, 1,
	// TimeUnit.MINUTES,
	// new LinkedBlockingQueue<Runnable>());

	public Uni<String> createEntry(ArrayListMultimap<String, String> headers, Map<String, Object> resolved) {
		return createTemporalEntity(headers, resolved, false);
	}

	Uni<String> createTemporalEntity(ArrayListMultimap<String, String> headers, Map<String, Object> resolved,
			boolean fromEntity) {

		CreateHistoryEntityRequest request;
		try {
			request = new CreateHistoryEntityRequest(headers, resolved, fromEntity);
			String tenantId = HttpUtils.getInternalTenant(headers);
			historyDAO.entityExists(request.getId(), tenantId);
			logger.trace("creating temporal entity");
		} catch (Exception e) {
			return Uni.createFrom().failure(e);

		}
		return handleRequest(request).combinedWith((t, u) -> {
			logger.debug("createMessage() :: completed");
			return request.getId();
		});

	}

//	private void pushToKafka(BaseRequest request) throws ResponseException {
//		if (historyToKafkaEnabled) {
//			kafkaExecutor.execute(new Runnable() {
//				@Override
//				public void run() {
//					kafkaTemplate.send(TEMP_TOPIC, request.getId(), new BaseRequest(request));
//
//				}
//			});
//		}
//	}

	private void pushToDB(HistoryEntityRequest request) throws ResponseException {
		try {
			historyDAO.storeTemporalEntity(request);
		} catch (SQLException e) {
			throw new ResponseException(ErrorType.InternalError, e.getLocalizedMessage());
		}

	}

	public Uni<Boolean> deleteEntry(ArrayListMultimap<String, String> headers, String entityId) {
		return delete(headers, entityId, null, null, null);
	}

	public Uni<Boolean> delete(ArrayListMultimap<String, String> headers, String entityId, String attributeId,
			String instanceId, Context linkHeaders) {
		DeleteHistoryEntityRequest request;
		try {
			historyDAO.getAllIds(entityId, HttpUtils.getInternalTenant(headers));
			logger.debug("deleting temporal entity with id : " + entityId + "and attributeId : " + attributeId);

			String resolvedAttrId = null;
			if (attributeId != null) {
				resolvedAttrId = ParamsResolver.expandAttribute(attributeId, linkHeaders);
			}
			request = new DeleteHistoryEntityRequest(headers, resolvedAttrId, instanceId, entityId);

		} catch (ResponseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return Uni.createFrom().failure(e);
		}
		return handleRequest(request).combinedWith((t, u) -> {
			logger.debug("delete Message() :: completed");
			return true;
		});
		// String tenantId = HttpUtils.getInternalTenant(headers);

		// if (!this.entityIds.containsEntry(tenantId, entityId)) {
		// throw new ResponseException(ErrorType.NotFound, entityId + " not found");
		// }
		// if (attributeId == null) {
		// this.entityIds.remove(tenantId, entityId);
		// }

	}

	// need to be check and change
	// endpoint "/entities/{entityId}/attrs"
	public Uni<UpdateResult> appendToEntry(ArrayListMultimap<String, String> headers, String entityId,
			Map<String, Object> resolved, String[] options) {
		// if (!this.entityIds.containsEntry(HttpUtils.getInternalTenant(headers),
		// entityId)) {
		// throw new ResponseException(ErrorType.NotFound, "You cannot create an
		// attribute on a none existing entity");
		// }
		AppendHistoryEntityRequest request;
		try {
			historyDAO.getAllIds(entityId, HttpUtils.getInternalTenant(headers));
			request = new AppendHistoryEntityRequest(headers, resolved, entityId);
		} catch (ResponseException e) {
			return Uni.createFrom().failure(e);

		}

		return handleRequest(request).combinedWith((t, u) -> {
			logger.debug("appendToEntry() :: completed");
			return request.getUpdateResult();
		});

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
		qp.setTenant(HttpUtils.getTenantFromHeaders(headers));
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
	public Uni<UpdateResult> updateEntry(ArrayListMultimap<String, String> headers, String entityId,
			Map<String, Object> entry) {
		// History can't do this
		throw new MethodNotFoundException();
	}

//	void handleRequest(HistoryEntityRequest request) throws ResponseException {
//		if (directDB) {
//			pushToDB(request);
//		}
//		pushToKafka(request);
//	}

	@SuppressWarnings("unchecked")
	private UniAndGroup2<Void, Void> handleRequest(HistoryEntityRequest request) {
		try {
			return Uni.combine().all().unis(historyDAO.storeTemporalEntity(request),
					kafkaSenderInterface.send(new BaseRequest(request)));
		} catch (SQLException e) {
			return (UniAndGroup2<Void, Void>) Uni.createFrom().failure(e);
		}
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
