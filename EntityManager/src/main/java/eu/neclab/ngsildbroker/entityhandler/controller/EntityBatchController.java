package eu.neclab.ngsildbroker.entityhandler.controller;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.results.NGSILDOperationResult;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestResponse;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.Lists;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.entityhandler.services.EntityService;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerRequest;

@Singleton
@Path("/ngsi-ld/v1/entityOperations")
public class EntityBatchController {

	@Inject
	EntityService entityService;

	@ConfigProperty(name = "batchoperations.maxnumber.create", defaultValue = "-1")
	int maxCreateBatch;

	@ConfigProperty(name = "batchoperations.maxnumber.update", defaultValue = "-1")
	int maxUpdateBatch;

	@ConfigProperty(name = "batchoperations.maxnumber.upsert", defaultValue = "-1")
	int maxUpsertBatch;

	@ConfigProperty(name = "batchoperations.maxnumber.delete", defaultValue = "-1")
	int maxDeleteBatch;

	@ConfigProperty(name = "ngsild.corecontext", defaultValue = "ngsild.corecontext:https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld")
	String coreContext;

	Random random = new Random();

	@PostConstruct
	public void init() {
		JsonLdProcessor.init(coreContext);
	}

	@POST
	@Path("/create")
	public Uni<RestResponse<Object>> createMultiple(HttpServerRequest request, String payload,
			@QueryParam("localOnly") boolean localOnly) {
		List<Map<String, Object>> compactedEntities;
		try {
			compactedEntities = (List<Map<String, Object>>) JsonUtils.fromString(payload);
		} catch (IOException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		List<Map<String, Object>> expandedEntities = Lists.newArrayList();
		List<Context> contexts = Lists.newArrayList();
		List<NGSILDOperationResult> fails = Lists.newArrayList();
		for (Map<String, Object> compactedEntity : compactedEntities) {
			try {
				Tuple2<Context, Map<String, Object>> tuple = HttpUtils.expandBody(request, compactedEntity,
						AppConstants.CREATE_REQUEST);
				expandedEntities.add(tuple.getItem2());
				contexts.add(tuple.getItem1());
			} catch (Exception e) {
				e.printStackTrace();
				NGSILDOperationResult failureResults = new NGSILDOperationResult(AppConstants.CREATE_REQUEST, (String) compactedEntity.get("id"));
				if (e instanceof ResponseException) {
					failureResults.addFailure((ResponseException) e);
				} else {
					failureResults.addFailure(new ResponseException(ErrorType.InvalidRequest, e.getMessage()));
				}
				fails.add(failureResults);
			}
		}
		return entityService.createBatch(HttpUtils.getTenant(request), expandedEntities, contexts, localOnly).onItem()
				.transform(opResults -> {
					opResults.addAll(fails);
					return HttpUtils.generateBatchResult(opResults);
				});
	}

	@POST
	@Path("/upsert")
	public Uni<RestResponse<Object>> upsertMultiple(HttpServerRequest request, String payload,
			@QueryParam(value = "options") String options, 	@QueryParam("localOnly") boolean localOnly) {
		List<Map<String, Object>> compactedEntities;
		try {
			compactedEntities = (List<Map<String, Object>>) JsonUtils.fromString(payload);
		} catch (IOException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		List<Map<String, Object>> expandedEntities = Lists.newArrayList();
		List<Context> contexts = Lists.newArrayList();
		List<NGSILDOperationResult> fails = Lists.newArrayList();
		for (Map<String, Object> compactedEntity : compactedEntities) {
			try {
				Tuple2<Context, Map<String, Object>> tuple = HttpUtils.expandBody(request, compactedEntity,
						AppConstants.ENTITY_CREATE_PAYLOAD);
				expandedEntities.add(tuple.getItem2());
				contexts.add(tuple.getItem1());
			} catch (Exception e) {
				NGSILDOperationResult failureResults = new NGSILDOperationResult(AppConstants.CREATE_REQUEST, (String) compactedEntity.get("id"));
				if (e instanceof ResponseException) {
					failureResults.addFailure((ResponseException) e);
				} else {
					failureResults.addFailure(new ResponseException(ErrorType.InvalidRequest, e.getMessage()));
				}
				fails.add(failureResults);
			}
		}
		return entityService.upsertBatch(HttpUtils.getTenant(request), expandedEntities, contexts, localOnly).onItem()
				.transform(opResults -> {
					opResults.addAll(fails);
					return HttpUtils.generateBatchResult(opResults);
				});
	}

	/**
	 * This is called update in the spec but compared to the single operations it is
	 * an append. All internal calls refer to this as appendBatch.
	 * 
	 * @param request
	 * @param payload
	 * @param options
	 * @return
	 */
	@POST
	@Path("/update")
	public Uni<RestResponse<Object>> appendMultiple(HttpServerRequest request, String payload,
			@QueryParam(value = "options") String options, @QueryParam("localOnly") boolean localOnly) {
		List<Map<String, Object>> compactedEntities;
		try {
			compactedEntities = (List<Map<String, Object>>) JsonUtils.fromString(payload);
		} catch (IOException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		List<Map<String, Object>> expandedEntities = Lists.newArrayList();
		List<Context> contexts = Lists.newArrayList();
		List<NGSILDOperationResult> fails = Lists.newArrayList();
		for (Map<String, Object> compactedEntity : compactedEntities) {
			try {
				Tuple2<Context, Map<String, Object>> tuple = HttpUtils.expandBody(request, compactedEntity,
						AppConstants.APPEND_REQUEST);
				expandedEntities.add(tuple.getItem2());
				contexts.add(tuple.getItem1());
			} catch (Exception e) {
				e.printStackTrace();
				NGSILDOperationResult failureResults = new NGSILDOperationResult(AppConstants.CREATE_REQUEST, (String) compactedEntity.get("id"));
				if (e instanceof ResponseException) {
					failureResults.addFailure((ResponseException) e);
				} else {
					failureResults.addFailure(new ResponseException(ErrorType.InvalidRequest, e.getMessage()));
				}
				fails.add(failureResults);
			}
		}
		return entityService.appendBatch(HttpUtils.getTenant(request), expandedEntities, contexts, localOnly).onItem()
				.transform(opResults -> {
					opResults.addAll(fails);
					return HttpUtils.generateBatchResult(opResults);
				});
	}

	@POST
	@Path("/delete")
	public Uni<RestResponse<Object>> deleteMultiple(HttpServerRequest request, String payload,	@QueryParam("localOnly") boolean localOnly) {
		List<String> entityIds;
		try {
			entityIds = (List<String>) JsonUtils.fromString(payload);
		} catch (IOException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		
		return entityService.deleteBatch(HttpUtils.getTenant(request), entityIds, localOnly).onItem()
				.transform(opResults -> {
					return HttpUtils.generateBatchResult(opResults);
				});
	}

}
