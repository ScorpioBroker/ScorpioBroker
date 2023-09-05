package eu.neclab.ngsildbroker.entityhandler.controller;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;

import eu.neclab.ngsildbroker.commons.datatypes.results.NGSILDOperationResult;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestResponse;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLDService;
import com.google.common.collect.Lists;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.entityhandler.services.EntityService;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.mutiny.tuples.Tuple3;
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

	@Inject
	JsonLDService ldService;

	@POST
	@Path("/create")
	public Uni<RestResponse<Object>> createMultiple(HttpServerRequest request,
			List<Map<String, Object>> compactedEntities, @QueryParam("localOnly") boolean localOnly) {
		List<Uni<Tuple2<String, Object>>> unis = Lists.newArrayList();
		for (Map<String, Object> compactedEntity : compactedEntities) {
			unis.add(HttpUtils.expandBody(request, compactedEntity, AppConstants.CREATE_REQUEST, ldService).onItem()
					.transform(i -> Tuple2.of((String) compactedEntity.get("id"), (Object) i)).onFailure()
					.recoverWithItem(e -> Tuple2.of((String) compactedEntity.get("id"), (Object) e)));
		}
		return Uni.combine().all().unis(unis).collectFailures().combinedWith(list -> {
			List<NGSILDOperationResult> fails = Lists.newArrayList();
			List<Map<String, Object>> expandedEntities = Lists.newArrayList();
			List<Context> contexts = Lists.newArrayList();
			for (Object obj : list) {
				Tuple2<String, Object> tuple = (Tuple2<String, Object>) obj;
				String entityId = tuple.getItem1();
				Object obj2 = tuple.getItem2();
				if (obj2 instanceof Exception) {
					NGSILDOperationResult failureResults = new NGSILDOperationResult(AppConstants.CREATE_REQUEST,
							entityId);
					if (obj2 instanceof ResponseException) {
						failureResults.addFailure((ResponseException) obj2);
					}
					else if(obj2 instanceof IOException){
						failureResults.addFailure(
								new ResponseException(ErrorType.LdContextNotAvailable, ((Exception) obj2).getMessage()));
					}
					else {
						failureResults.addFailure(
								new ResponseException(ErrorType.InvalidRequest, ((Exception) obj2).getMessage()));
					}
					fails.add(failureResults);
				} else {
					Tuple2<Context, Map<String, Object>> tuple2 = (Tuple2<Context, Map<String, Object>>) obj2;
					expandedEntities.add(tuple2.getItem2());
					contexts.add(tuple2.getItem1());
				}
			}
			return Tuple3.of(fails, expandedEntities, contexts);

		}).onItem().transformToUni(tuple -> {
			List<NGSILDOperationResult> fails = tuple.getItem1();
			List<Map<String, Object>> expandedEntities = tuple.getItem2();
			List<Context> contexts = tuple.getItem3();
			return entityService.createBatch(HttpUtils.getTenant(request), expandedEntities, contexts, localOnly)
					.onItem().transform(opResults -> {
						opResults.addAll(fails);
						return HttpUtils.generateBatchResult(opResults);
					});
		}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);

	}

	@POST
	@Path("/upsert")
	public Uni<RestResponse<Object>> upsertMultiple(HttpServerRequest request,
			List<Map<String, Object>> compactedEntities, @QueryParam(value = "options") String options,
			@QueryParam("localOnly") boolean localOnly) {
		List<Uni<Tuple2<String, Object>>> unis = Lists.newArrayList();
		for (Map<String, Object> compactedEntity : compactedEntities) {
			unis.add(HttpUtils.expandBody(request, compactedEntity, AppConstants.CREATE_REQUEST, ldService).onItem()
					.transform(i -> Tuple2.of((String) compactedEntity.get("id"), (Object) i)).onFailure()
					.recoverWithItem(e -> Tuple2.of((String) compactedEntity.get("id"), (Object) e)));
		}
		return Uni.combine().all().unis(unis).collectFailures().combinedWith(list -> {
			List<NGSILDOperationResult> fails = Lists.newArrayList();
			List<Map<String, Object>> expandedEntities = Lists.newArrayList();
			List<Context> contexts = Lists.newArrayList();
			for (Object obj : list) {
				Tuple2<String, Object> tuple = (Tuple2<String, Object>) obj;
				String entityId = tuple.getItem1();
				Object obj2 = tuple.getItem2();
				if (obj2 instanceof Exception) {
					NGSILDOperationResult failureResults = new NGSILDOperationResult(AppConstants.CREATE_REQUEST,
							entityId);
					if (obj2 instanceof ResponseException) {
						failureResults.addFailure((ResponseException) obj2);
					} else {
						failureResults.addFailure(
								new ResponseException(ErrorType.InvalidRequest, ((Exception) obj2).getMessage()));
					}
					fails.add(failureResults);
				} else {
					Tuple2<Context, Map<String, Object>> tuple2 = (Tuple2<Context, Map<String, Object>>) obj2;
					expandedEntities.add(tuple2.getItem2());
					contexts.add(tuple2.getItem1());
				}
			}
			return Tuple3.of(fails, expandedEntities, contexts);

		}).onItem().transformToUni(tuple -> {
			List<NGSILDOperationResult> fails = tuple.getItem1();
			List<Map<String, Object>> expandedEntities = tuple.getItem2();
			List<Context> contexts = tuple.getItem3();
			return entityService.upsertBatch(HttpUtils.getTenant(request), expandedEntities, contexts, localOnly)
					.onItem().transform(opResults -> {
						opResults.addAll(fails);
						return HttpUtils.generateBatchResult(opResults);
					});
		}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);

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
	public Uni<RestResponse<Object>> appendMultiple(HttpServerRequest request,
			List<Map<String, Object>> compactedEntities, @QueryParam(value = "options") String options,
			@QueryParam("localOnly") boolean localOnly) {
		List<Uni<Tuple2<String, Object>>> unis = Lists.newArrayList();
		for (Map<String, Object> compactedEntity : compactedEntities) {
			unis.add(HttpUtils.expandBody(request, compactedEntity, AppConstants.APPEND_REQUEST, ldService).onItem()
					.transform(i -> Tuple2.of((String) compactedEntity.get("id"), (Object) i)).onFailure()
					.recoverWithItem(e -> Tuple2.of((String) compactedEntity.get("id"), (Object) e)));
		}
		return Uni.combine().all().unis(unis).collectFailures().combinedWith(list -> {
			List<NGSILDOperationResult> fails = Lists.newArrayList();
			List<Map<String, Object>> expandedEntities = Lists.newArrayList();
			List<Context> contexts = Lists.newArrayList();
			for (Object obj : list) {
				Tuple2<String, Object> tuple = (Tuple2<String, Object>) obj;
				String entityId = tuple.getItem1();
				Object obj2 = tuple.getItem2();
				if (obj2 instanceof Exception) {
					NGSILDOperationResult failureResults = new NGSILDOperationResult(AppConstants.APPEND_REQUEST,
							entityId);
					if (obj2 instanceof ResponseException) {
						failureResults.addFailure((ResponseException) obj2);
					} else {
						failureResults.addFailure(
								new ResponseException(ErrorType.InvalidRequest, ((Exception) obj2).getMessage()));
					}
					fails.add(failureResults);
				} else {
					Tuple2<Context, Map<String, Object>> tuple2 = (Tuple2<Context, Map<String, Object>>) obj2;
					expandedEntities.add(tuple2.getItem2());
					contexts.add(tuple2.getItem1());
				}
			}
			return Tuple3.of(fails, expandedEntities, contexts);

		}).onItem().transformToUni(tuple -> {
			List<NGSILDOperationResult> fails = tuple.getItem1();
			List<Map<String, Object>> expandedEntities = tuple.getItem2();
			List<Context> contexts = tuple.getItem3();
			return entityService.appendBatch(HttpUtils.getTenant(request), expandedEntities, contexts, localOnly)
					.onItem().transform(opResults -> {
						opResults.addAll(fails);
						return HttpUtils.generateBatchResult(opResults);
					});
		}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
	}

	@POST
	@Path("/delete")
	public Uni<RestResponse<Object>> deleteMultiple(HttpServerRequest request, List<String> entityIds,
			@QueryParam("localOnly") boolean localOnly) {
		return entityService.deleteBatch(HttpUtils.getTenant(request), entityIds, localOnly).onItem()
				.transform(opResults -> {
					return HttpUtils.generateBatchResult(opResults);
				}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
	}

}
