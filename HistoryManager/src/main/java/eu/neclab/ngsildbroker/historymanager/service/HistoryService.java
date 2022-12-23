package eu.neclab.ngsildbroker.historymanager.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.el.MethodNotFoundException;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.jsonldjava.core.Context;
import com.google.common.collect.ArrayListMultimap;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.BatchInfo;
import eu.neclab.ngsildbroker.commons.datatypes.QueryParams;
import eu.neclab.ngsildbroker.commons.datatypes.requests.AppendHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.HistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.UpdateHistoryEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.results.CreateResult;
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
import io.smallrye.mutiny.unchecked.Unchecked;
import io.smallrye.reactive.messaging.MutinyEmitter;

@Singleton
public class HistoryService extends BaseQueryService implements EntryCRUDService {

	private final static Logger logger = LoggerFactory.getLogger(HistoryService.class);

	@Inject
	HistoryDAO historyDAO;

	@ConfigProperty(name = "scorpio.directDB", defaultValue = "true")
	boolean directDB;

	@ConfigProperty(name = "scorpio.topics.temporal")
	String TEMP_TOPIC;

	@ConfigProperty(name = "scorpio.history.tokafka", defaultValue = "false")
	boolean historyToKafkaEnabled;

	@Inject
	@Channel(AppConstants.HISTORY_CHANNEL)
	MutinyEmitter<BaseRequest> kafkaSenderInterface;

	public Uni<CreateResult> createEntry(String tenant, Map<String, Object> resolved) {
		return createEntry(tenant, resolved, new BatchInfo(-1, -1));
	}

	public Uni<CreateResult> createEntry(String tenant, Map<String, Object> resolved, BatchInfo batchInfo) {
		return createTemporalEntity(tenant, resolved, false, batchInfo);
	}

	Uni<CreateResult> createTemporalEntity(String tenant, Map<String, Object> resolved, boolean fromEntity,
			BatchInfo batchInfo) {
		logger.trace("creating temporal entity");
		CreateHistoryEntityRequest request;
		try {
			request = new CreateHistoryEntityRequest(tenant, resolved, fromEntity);
		} catch (ResponseException e) {
			return Uni.createFrom().failure(e);
		}
		return historyDAO.isTempEntityExist(request.getId(), tenant).onItem()
				.transformToUni(t2 -> {
					return handleRequest(request).combinedWith((t, u) -> {
						logger.debug("createMessage() :: completed");
						return new CreateResult(request.getId(), Boolean.parseBoolean(t2.toString()));
					});
				});
	}

	public Uni<Boolean> deleteEntry(String tenant, String entityId) {
		return deleteEntry(tenant, entityId, new BatchInfo(-1, -1));
	}

	public Uni<Boolean> deleteEntry(String tenant, String entityId, BatchInfo batchInfo) {
		return delete(tenant, entityId, null, null, null);
	}

	public Uni<Boolean> delete(String tenant, String entityId, String attributeId, String instanceId,
			Context linkHeaders) {
		if (entityId == null) {
			return Uni.createFrom()
					.failure(new ResponseException(ErrorType.BadRequestData, "empty entity id not allowed"));
		}

		return Uni.combine().all()
				.unis(historyDAO.getTemporalEntity(entityId, tenant), historyDAO
						.temporalEntityAttrInstanceExist(entityId, tenant, attributeId, instanceId, linkHeaders))
				.asTuple().onItem().transform(Unchecked.function(t -> {
					String resolvedAttrId = null;
					if (attributeId != null) {
						resolvedAttrId = ParamsResolver.expandAttribute(attributeId, linkHeaders);
					}
					return new DeleteHistoryEntityRequest(tenant, resolvedAttrId, instanceId, entityId);

				})).onItem().transformToUni(t2 -> {
					return handleRequest(t2).combinedWith((t, u) -> {
						logger.debug("delete Message() :: completed");
						return true;
					});
				});
	}

	public Uni<UpdateResult> appendToEntry(String tenant, String entityId, Map<String, Object> resolved,
			String[] options) {
		return appendToEntry(tenant, entityId, resolved, options, new BatchInfo(-1, -1));
	}

	public Uni<UpdateResult> appendToEntry(String tenant, String entityId, Map<String, Object> resolved,
			String[] options, BatchInfo batchInfo) {
		return historyDAO.getTemporalEntity(entityId, tenant).onItem().transformToUni(t -> {
			try {
				return Uni.createFrom().item(new AppendHistoryEntityRequest(tenant, resolved, entityId));
			} catch (ResponseException e) {
				return Uni.createFrom().failure(e);
			}
		}).onItem().transformToUni(t2 -> {
			return handleRequest(t2).combinedWith((t, u) -> {
				logger.debug("appendToEntry() :: completed");
				return t2.getUpdateResult();
			});
		});
	}

	// for endpoint "entities/{entityId}/attrs/{attrId}/{instanceId}")
	public Uni<Void> modifyAttribInstanceTemporalEntity(String tenant, String entityId, Map<String, Object> resolved,
			String attribId, String instanceId, Context linkHeaders) {

		String resolvedAttrId;
		try {
			resolvedAttrId = ParamsResolver.expandAttribute(attribId, linkHeaders);
		} catch (ResponseException e) {
			return Uni.createFrom().failure(e);
		}
		QueryParams qp = new QueryParams();
		List<Map<String, String>> temp1 = new ArrayList<Map<String, String>>();
		HashMap<String, String> temp2 = new HashMap<String, String>();
		temp2.put(NGSIConstants.JSON_LD_ID, entityId);
		temp1.add(temp2);
		qp.setEntities(temp1);
		qp.setAttrs(resolvedAttrId);
		qp.setInstanceId(instanceId);
		qp.setIncludeSysAttrs(true);
		qp.setTenant(tenant);

		return historyDAO.query(qp).onItem().transformToUni(queryResult -> {
			List<Map<String, Object>> entityList = ((QueryResult) queryResult).getData();
			if (entityList.size() == 0) {
				return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound, "Entity not found"));
			}
			UpdateHistoryEntityRequest request;
			try {
				request = new UpdateHistoryEntityRequest(tenant, resolved, entityId, resolvedAttrId, instanceId,
						entityList);
			} catch (ResponseException e) {
				return Uni.createFrom().failure(e);
			}
			return handleRequest(request).combinedWith(t -> null);
		});

	}

	@Override
	public Uni<UpdateResult> updateEntry(String tenant, String entityId, Map<String, Object> entry) {
		return updateEntry(tenant, entityId, entry, new BatchInfo(-1, -1));
	}

	@Override
	public Uni<UpdateResult> updateEntry(String tenant, String entityId, Map<String, Object> entry,
			BatchInfo batchInfo) {
		// History can't do this
		throw new MethodNotFoundException();
	}

	public UniAndGroup2<CreateResult, Void> handleRequest(HistoryEntityRequest request) {
		return Uni.combine().all().unis(historyDAO.storeTemporalEntity(request), sendToKafka(request));
	}

	private Uni<Void> sendToKafka(HistoryEntityRequest request) {
		if (historyToKafkaEnabled) {
			return kafkaSenderInterface.send(request);
		} else {
			return Uni.createFrom().voidItem();
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

	@Override
	public Uni<Void> sendFail(BatchInfo batchInfo) {
		return Uni.createFrom().voidItem();
	}

}
