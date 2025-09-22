package eu.neclab.ngsildbroker.entityhandler.controller;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;
import eu.neclab.ngsildbroker.commons.datatypes.ViaHeaders;
import eu.neclab.ngsildbroker.commons.datatypes.results.NGSILDOperationResult;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestResponse;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLDService;
import com.google.common.collect.Lists;
import com.google.common.net.HttpHeaders;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import eu.neclab.ngsildbroker.entityhandler.services.EntityService;
import io.quarkus.runtime.Startup;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.mutiny.tuples.Tuple3;
import io.vertx.core.http.HttpServerRequest;

@ApplicationScoped
@Startup
@Path("/ngsi-ld/v1/entityOperations")
@SuppressWarnings("unchecked")
public class EntityBatchController {

	@Inject
	EntityService entityService;

	@ConfigProperty(name = "scorpio.entity.batch-operations.create.max")
	int maxCreateBatch;

	@ConfigProperty(name = "scorpio.entity.batch-operations.update.max")
	int maxUpdateBatch;

	@ConfigProperty(name = "scorpio.entity.batch-operations.upsert.max")
	int maxUpsertBatch;

	@ConfigProperty(name = "scorpio.entity.batch-operations.delete.max")
	int maxDeleteBatch;

	@ConfigProperty(name = "scorpio.ngsild.corecontext")
	String coreContext;

	Random random = new Random();

	@Inject
	JsonLDService ldService;

	@Inject
	ObjectMapper objectMapper;

	@Inject
	MicroServiceUtils microServiceUtils;

	@POST
	@Path("/create")
	public Uni<RestResponse<Object>> createMultiple(HttpServerRequest request, String body,
			@QueryParam("localOnly") String localOnlyS) {
		List<Uni<Tuple2<String, Object>>> unis = Lists.newArrayList();
		List<Map<String, Object>> compactedEntities;
		boolean localOnly;
		String tenant = HttpUtils.getTenant(request);
		try {
			localOnly = HttpUtils.parseBoolean(localOnlyS);
			compactedEntities = new JsonArray(body).getList();
		} catch (DecodeException | ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		if (compactedEntities == null || compactedEntities.isEmpty()) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(
					new ResponseException(ErrorType.BadRequestData), tenant));
		}
		ViaHeaders viaHeaders;
		try {
			viaHeaders = new ViaHeaders(request.headers().getAll(HttpHeaders.VIA),
					microServiceUtils.getSourceAlias(tenant));
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		for (Map<String, Object> compactedEntity : compactedEntities) {
			// try {
			// noConcise(compactedEntity);
			// } catch (ResponseException e) {
			// unis.add(Uni.createFrom().item(Tuple2.of((String) compactedEntity.get("id"),
			// (Object) e)));
			// continue;
			// }
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
							entityId, tenant);
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
					.createBatch(tenant, expandedEntities, contexts, localOnly, request.headers(),
							viaHeaders)
					.onItem().transform(opResults -> {
						opResults.addAll(fails);
						return HttpUtils.generateBatchResult(opResults);
					});
		}).onFailure().recoverWithItem(e -> {
			return HttpUtils.handleControllerExceptions(e, tenant);
		});

	}

	@POST
	@Path("/upsert")
	public Uni<RestResponse<Object>> upsertMultiple(HttpServerRequest request, String body,
			@QueryParam(value = "options") String options, @QueryParam("localOnly") String localOnlyS) {
		boolean localOnly;
		List<Map<String, Object>> compactedEntities;
		String tenant = HttpUtils.getTenant(request);
		try {
			localOnly = HttpUtils.parseBoolean(localOnlyS);
			compactedEntities = new JsonArray(body).getList();
		} catch (DecodeException | ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		boolean doReplace;
		if (options != null && !options.isEmpty()) {
			List<String> optionsList = Arrays.asList(options.split(","));
			doReplace = !optionsList.contains("update");
		} else {
			doReplace = true;
		}
		ViaHeaders viaHeaders;
		try {
			viaHeaders = new ViaHeaders(request.headers().getAll(HttpHeaders.VIA),
					microServiceUtils.getSourceAlias(tenant));
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		List<Uni<Tuple2<String, Object>>> unis = Lists.newArrayList();
		for (Map<String, Object> compactedEntity : compactedEntities) {
			// try {
			// noConcise(compactedEntity);
			// } catch (ResponseException e) {
			// unis.add(Uni.createFrom().item(Tuple2.of((String) compactedEntity.get("id"),
			// (Object) e)));
			// continue;
			// };
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
							entityId, tenant);
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
			return entityService.upsertBatch(tenant, expandedEntities, contexts, localOnly,
					doReplace, request.headers(), viaHeaders).onItem().transform(opResults -> {
						opResults.addAll(fails);
						return HttpUtils.generateBatchResult(opResults);
					});
		}).onFailure().recoverWithItem(e -> {
			return HttpUtils.handleControllerExceptions(e, tenant);
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
		String tenant = HttpUtils.getTenant(request);
		boolean localOnly;
		try {
			localOnly = HttpUtils.parseBoolean(localOnlyS);
			compactedEntities = new JsonArray(body).getList();
		} catch (DecodeException | ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		boolean isNoOverwrite = options != null && options.contains(NGSIConstants.NO_OVERWRITE_OPTION);
		List<Uni<Tuple2<String, Object>>> unis = Lists.newArrayList();
		for (Map<String, Object> compactedEntity : compactedEntities) {
			// try {
			// noConcise(compactedEntity);
			// } catch (ResponseException e) {
			// unis.add(Uni.createFrom().item(Tuple2.of((String) compactedEntity.get("id"),
			// (Object) e)));
			// continue;
			// };
			unis.add(HttpUtils.expandBody(request, compactedEntity, AppConstants.APPEND_REQUEST, ldService).onItem()
					.transform(i -> Tuple2.of((String) compactedEntity.get("id"), (Object) i)).onFailure()
					.recoverWithItem(e -> Tuple2.of((String) compactedEntity.get("id"), (Object) e)));
		}

		ViaHeaders viaHeaders;
		try {
			viaHeaders = new ViaHeaders(request.headers().getAll(HttpHeaders.VIA),
					microServiceUtils.getSourceAlias(tenant));
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
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
							entityId, tenant);
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
			return entityService.appendBatch(tenant, expandedEntities, contexts, localOnly,
					isNoOverwrite, request.headers(), viaHeaders).onItem().transform(opResults -> {
						opResults.addAll(fails);
						return HttpUtils.generateBatchResult(opResults);
					});
		}).onFailure().recoverWithItem(e -> {
			return HttpUtils.handleControllerExceptions(e, tenant);
		});
	}

	@POST
	@Path("/delete")
	public Uni<RestResponse<Object>> deleteMultiple(HttpServerRequest request, String entityIdsStr,
			@QueryParam("localOnly") String localOnlyS) {
		String tenant = HttpUtils.getTenant(request);
		List<String> entityIds;
		boolean localOnly;
		try {
			localOnly = HttpUtils.parseBoolean(localOnlyS);
			entityIds = new JsonArray(entityIdsStr).getList();
		} catch (DecodeException | ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		if (entityIds.isEmpty()) {
			return Uni.createFrom()
					.item(HttpUtils.handleControllerExceptions(
							new ResponseException(ErrorType.BadRequestData, "Empty ID arrays are not allowed"),
							tenant));
		}
		ViaHeaders viaHeaders;
		try {
			viaHeaders = new ViaHeaders(request.headers().getAll(HttpHeaders.VIA),
					microServiceUtils.getSourceAlias(tenant));
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		return entityService
				.deleteBatch(tenant, entityIds, localOnly, request.headers(), viaHeaders).onItem()
				.transform(opResults -> {
					return HttpUtils.generateBatchResult(opResults);
				}).onFailure().recoverWithItem(e -> {
					return HttpUtils.handleControllerExceptions(e, tenant);
				});
	}

	@POST
	@Path("/merge")
	public Uni<RestResponse<Object>> mergeMultiple(HttpServerRequest request, String body,
			@QueryParam(value = "options") String options, @QueryParam("localOnly") String localOnlyS) {
		List<Map<String, Object>> compactedEntities;
		String tenant = HttpUtils.getTenant(request);
		boolean localOnly;
		try {
			localOnly = HttpUtils.parseBoolean(localOnlyS);
			compactedEntities = new JsonArray(body).getList();
		} catch (DecodeException | ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		boolean isNoOverwrite = options != null && options.contains(NGSIConstants.NO_OVERWRITE_OPTION);
		List<Uni<Tuple2<String, Object>>> unis = Lists.newArrayList();
		for (Map<String, Object> compactedEntity : compactedEntities) {
			// try {
			// noConcise(compactedEntity);
			// } catch (ResponseException e) {
			// unis.add(Uni.createFrom().item(Tuple2.of((String) compactedEntity.get("id"),
			// (Object) e)));
			// continue;
			// };
			unis.add(HttpUtils.expandBody(request, compactedEntity, AppConstants.MERGE_PATCH_REQUEST, ldService)
					.onItem().transform(i -> Tuple2.of((String) compactedEntity.get("id"), (Object) i)).onFailure()
					.recoverWithItem(e -> Tuple2.of((String) compactedEntity.get("id"), (Object) e)));
		}
		ViaHeaders viaHeaders;
		try {
			viaHeaders = new ViaHeaders(request.headers().getAll(HttpHeaders.VIA),
					microServiceUtils.getSourceAlias(tenant));
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
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
							entityId, tenant);
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
			return entityService.mergeBatch(tenant, expandedEntities, contexts, localOnly,
					isNoOverwrite, request.headers(), viaHeaders).onItem().transform(opResults -> {
						opResults.addAll(fails);
						return HttpUtils.generateBatchResult(opResults);
					});
		}).onFailure().recoverWithItem(e -> {
			return HttpUtils.handleControllerExceptions(e, tenant);
		});
	}

}
