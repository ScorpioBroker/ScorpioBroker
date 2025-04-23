package eu.neclab.ngsildbroker.entityhandler.controller;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;

import eu.neclab.ngsildbroker.commons.datatypes.results.NGSILDOperationResult;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestResponse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLDService;
import com.github.jsonldjava.utils.JsonUtils;
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
@SuppressWarnings("unchecked")
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

	@Inject
	ObjectMapper objectMapper;

	@POST
	@Path("/create")
	public Uni<RestResponse<Object>> createMultiple(HttpServerRequest request, String body,
			@QueryParam("localOnly") String localOnlyS) {
		List<Uni<Tuple2<String, Object>>> unis = Lists.newArrayList();
		List<Map<String, Object>> compactedEntities;
		boolean localOnly;
		try {
			localOnly = HttpUtils.parseBoolean(localOnlyS);
			compactedEntities = new JsonArray(body).getList();
		} catch (DecodeException | ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
		}
		if (compactedEntities == null || compactedEntities.isEmpty()) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(
					new ResponseException(ErrorType.BadRequestData), HttpUtils.getTenant(request)));
		}
		for (Map<String, Object> compactedEntity : compactedEntities) {
//			try {
//				noConcise(compactedEntity);
//			} catch (ResponseException e) {
//				unis.add(Uni.createFrom().item(Tuple2.of((String) compactedEntity.get("id"), (Object) e)));
//				continue;
//			}
			unis.add(HttpUtils.expandBody(request, compactedEntity, AppConstants.CREATE_REQUEST, ldService).onItem()
					.transform(i -> Tuple2.of((String) compactedEntity.get("id"), (Object) i)).onFailure()
					.recoverWithItem(e -> Tuple2.of((String) compactedEntity.get("id"), (Object) e)));
		}
		return Uni.combine().all().unis(unis).collectFailures().with(list -> {
			List<NGSILDOperationResult> fails = Lists.newArrayList();
			List<Map<String, Object>> expandedEntities = Lists.newArrayList();
			List<Context> contexts = Lists.newArrayList();
			for (Object obj : list) {
				Tuple2<String, Object> tuple = (Tuple2<String, Object>) obj;
				String entityId = tuple.getItem1();
				Object obj2 = tuple.getItem2();
				if (obj2 instanceof Exception) {
					NGSILDOperationResult failureResults = new NGSILDOperationResult(AppConstants.CREATE_REQUEST,
							entityId, HttpUtils.getTenant(request));
					if (obj2 instanceof ResponseException) {
						failureResults.addFailure((ResponseException) obj2);
					} else if (obj2 instanceof IOException) {
						failureResults.addFailure(new ResponseException(ErrorType.LdContextNotAvailable,
								((Exception) obj2).getMessage()));
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
			if (expandedEntities.isEmpty()) {
				return Uni.createFrom().item(fails).onItem().transform(HttpUtils::generateBatchResult);
			}
			return entityService
					.createBatch(HttpUtils.getTenant(request), expandedEntities, contexts, localOnly, request.headers())
					.onItem().transform(opResults -> {
						opResults.addAll(fails);
						return HttpUtils.generateBatchResult(opResults);
					});
		}).onFailure().recoverWithItem(e -> {
			return HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request));
		});

	}

	@POST
	@Path("/upsert")
	public Uni<RestResponse<Object>> upsertMultiple(HttpServerRequest request, String body,
			@QueryParam(value = "options") String options, @QueryParam("localOnly") String localOnlyS) {
		boolean localOnly;
		List<Map<String, Object>> compactedEntities;
		try {
			localOnly = HttpUtils.parseBoolean(localOnlyS);
			compactedEntities = new JsonArray(body).getList();
		} catch (DecodeException | ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
		}
		boolean doReplace;
		if (options != null && !options.isEmpty()) {
			List<String> optionsList = Arrays.asList(options.split(","));
			doReplace = !optionsList.contains("update");
		} else {
			doReplace = true;
		}
		List<Uni<Tuple2<String, Object>>> unis = Lists.newArrayList();
		for (Map<String, Object> compactedEntity : compactedEntities) {
//			try {
//				noConcise(compactedEntity);
//			} catch (ResponseException e) {
//				unis.add(Uni.createFrom().item(Tuple2.of((String) compactedEntity.get("id"), (Object) e)));
//				continue;
//			};
			unis.add(HttpUtils.expandBody(request, compactedEntity, AppConstants.CREATE_REQUEST, ldService).onItem()
					.transform(i -> Tuple2.of((String) compactedEntity.get("id"), (Object) i)).onFailure()
					.recoverWithItem(e -> Tuple2.of((String) compactedEntity.get("id"), (Object) e)));
		}
		return Uni.combine().all().unis(unis).collectFailures().with(list -> {
			List<NGSILDOperationResult> fails = Lists.newArrayList();
			List<Map<String, Object>> expandedEntities = Lists.newArrayList();
			List<Context> contexts = Lists.newArrayList();
			for (Object obj : list) {
				Tuple2<String, Object> tuple = (Tuple2<String, Object>) obj;
				String entityId = tuple.getItem1();
				Object obj2 = tuple.getItem2();
				if (obj2 instanceof Exception) {
					NGSILDOperationResult failureResults = new NGSILDOperationResult(AppConstants.UPSERT_REQUEST,
							entityId, HttpUtils.getTenant(request));
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
			return entityService.upsertBatch(HttpUtils.getTenant(request), expandedEntities, contexts, localOnly,
					doReplace, request.headers()).onItem().transform(opResults -> {
						opResults.addAll(fails);
						return HttpUtils.generateBatchResult(opResults);
					});
		}).onFailure().recoverWithItem(e -> {
			return HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request));
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
	public Uni<RestResponse<Object>> appendMultiple(HttpServerRequest request, String body,
			@QueryParam(value = "options") String options, @QueryParam("localOnly") String localOnlyS) {
		List<Map<String, Object>> compactedEntities;
		boolean localOnly;
		try {
			localOnly = HttpUtils.parseBoolean(localOnlyS);
			compactedEntities = new JsonArray(body).getList();
		} catch (DecodeException | ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
		}
		boolean isNoOverwrite = options != null && options.contains(NGSIConstants.NO_OVERWRITE_OPTION);
		List<Uni<Tuple2<String, Object>>> unis = Lists.newArrayList();
		for (Map<String, Object> compactedEntity : compactedEntities) {
//			try {
//				noConcise(compactedEntity);
//			} catch (ResponseException e) {
//				unis.add(Uni.createFrom().item(Tuple2.of((String) compactedEntity.get("id"), (Object) e)));
//				continue;
//			};
			unis.add(HttpUtils.expandBody(request, compactedEntity, AppConstants.APPEND_REQUEST, ldService).onItem()
					.transform(i -> Tuple2.of((String) compactedEntity.get("id"), (Object) i)).onFailure()
					.recoverWithItem(e -> Tuple2.of((String) compactedEntity.get("id"), (Object) e)));
		}
		return Uni.combine().all().unis(unis).collectFailures().with(list -> {
			List<NGSILDOperationResult> fails = Lists.newArrayList();
			List<Map<String, Object>> expandedEntities = Lists.newArrayList();
			List<Context> contexts = Lists.newArrayList();
			for (Object obj : list) {
				Tuple2<String, Object> tuple = (Tuple2<String, Object>) obj;
				String entityId = tuple.getItem1();
				Object obj2 = tuple.getItem2();
				if (obj2 instanceof Exception) {
					NGSILDOperationResult failureResults = new NGSILDOperationResult(AppConstants.APPEND_REQUEST,
							entityId, HttpUtils.getTenant(request));
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
			return entityService.appendBatch(HttpUtils.getTenant(request), expandedEntities, contexts, localOnly,
					isNoOverwrite, request.headers()).onItem().transform(opResults -> {
						opResults.addAll(fails);
						return HttpUtils.generateBatchResult(opResults);
					});
		}).onFailure().recoverWithItem(e -> {
			return HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request));
		});
	}

	@POST
	@Path("/delete")
	public Uni<RestResponse<Object>> deleteMultiple(HttpServerRequest request, String entityIdsStr,
			@QueryParam("localOnly") String localOnlyS) {
		List<String> entityIds;
		boolean localOnly;
		try {
			localOnly = HttpUtils.parseBoolean(localOnlyS);
			entityIds = new JsonArray(entityIdsStr).getList();
		} catch (DecodeException | ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
		}
		if (entityIds.isEmpty()) {
			return Uni.createFrom()
					.item(HttpUtils.handleControllerExceptions(
							new ResponseException(ErrorType.BadRequestData, "Empty ID arrays are not allowed"),
							HttpUtils.getTenant(request)));
		}
		return entityService.deleteBatch(HttpUtils.getTenant(request), entityIds, localOnly, request.headers()).onItem()
				.transform(opResults -> {
					return HttpUtils.generateBatchResult(opResults);
				}).onFailure().recoverWithItem(e -> {
					return HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request));
				});
	}

	@POST
	@Path("/merge")
	public Uni<RestResponse<Object>> mergeMultiple(HttpServerRequest request, String body,
			@QueryParam(value = "options") String options, @QueryParam("localOnly") String localOnlyS) {
		List<Map<String, Object>> compactedEntities;
		boolean localOnly;
		try {
			localOnly = HttpUtils.parseBoolean(localOnlyS);
			compactedEntities = new JsonArray(body).getList();
		} catch (DecodeException | ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
		}
		boolean isNoOverwrite = options != null && options.contains(NGSIConstants.NO_OVERWRITE_OPTION);
		List<Uni<Tuple2<String, Object>>> unis = Lists.newArrayList();
		for (Map<String, Object> compactedEntity : compactedEntities) {
//			try {
//				noConcise(compactedEntity);
//			} catch (ResponseException e) {
//				unis.add(Uni.createFrom().item(Tuple2.of((String) compactedEntity.get("id"), (Object) e)));
//				continue;
//			};
			unis.add(HttpUtils.expandBody(request, compactedEntity, AppConstants.MERGE_PATCH_REQUEST, ldService)
					.onItem().transform(i -> Tuple2.of((String) compactedEntity.get("id"), (Object) i)).onFailure()
					.recoverWithItem(e -> Tuple2.of((String) compactedEntity.get("id"), (Object) e)));
		}
		return Uni.combine().all().unis(unis).collectFailures().with(list -> {
			List<NGSILDOperationResult> fails = Lists.newArrayList();
			List<Map<String, Object>> expandedEntities = Lists.newArrayList();
			List<Context> contexts = Lists.newArrayList();
			for (Object obj : list) {
				Tuple2<String, Object> tuple = (Tuple2<String, Object>) obj;
				String entityId = tuple.getItem1();
				Object obj2 = tuple.getItem2();
				if (obj2 instanceof Exception) {
					NGSILDOperationResult failureResults = new NGSILDOperationResult(AppConstants.APPEND_REQUEST,
							entityId, HttpUtils.getTenant(request));
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
			return entityService.mergeBatch(HttpUtils.getTenant(request), expandedEntities, contexts, localOnly,
					isNoOverwrite, request.headers()).onItem().transform(opResults -> {
						opResults.addAll(fails);
						return HttpUtils.generateBatchResult(opResults);
					});
		}).onFailure().recoverWithItem(e -> {
			return HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request));
		});
	}

}
