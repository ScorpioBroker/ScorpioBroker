package eu.neclab.ngsildbroker.historyentitymanager.controller;

import java.util.Map;

import jakarta.inject.Inject;
import jakarta.enterprise.context.ApplicationScoped;
import io.quarkus.runtime.Startup;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.PATCH;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.metrics.MetricUnits;
import org.eclipse.microprofile.metrics.annotation.ConcurrentGauge;
import org.eclipse.microprofile.metrics.annotation.Counted;
import org.eclipse.microprofile.metrics.annotation.Timed;
import org.jboss.resteasy.reactive.RestResponse;

import com.github.jsonldjava.core.JsonLDService;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.historyentitymanager.service.HistoryEntityService;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;

@ApplicationScoped
@Startup
@Path("/ngsi-ld/v1/temporal/entities")
public class HistoryController {

	@Inject
	HistoryEntityService historyService;

	@ConfigProperty(name = "scorpio.history.default-limit")
	int defaultLimit;
	@ConfigProperty(name = "scorpio.history.max-limit")
	int maxLimit;

	@ConfigProperty(name = "scorpio.ngsild.corecontext")
	String coreContext;

	@Inject
	JsonLDService ldService;

	@POST
	@Counted(name = "temp_entity_create_total", description = "Total number of temp entity create requests", absolute = true)
	@Timed(name = "temp_entity_create_duration", description = "Duration of temp entity create requests", unit = MetricUnits.MILLISECONDS, absolute = true)
	@ConcurrentGauge(name = "temp_entity_create_concurrent", description = "Number of concurrent temp entity create requests", absolute = true)
	public Uni<RestResponse<Object>> createTemporalEntity(HttpServerRequest request, String body) {
		Map<String, Object> payload;
		try {
			payload = new JsonObject(body).getMap();
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
		}
		return HttpUtils.expandBody(request, payload, AppConstants.TEMP_ENTITY_CREATE_PAYLOAD, ldService).onItem()
				.transformToUni(tuple -> {
					return historyService.createEntry(HttpUtils.getTenant(request), tuple.getItem2(), tuple.getItem1(),
							request.headers()).onItem().transform(opResult -> {
								if (opResult.isWasUpdated()) {
									return HttpUtils.generateUpdateResultResponse(opResult);
								}
								return HttpUtils.generateCreateResult(opResult, AppConstants.HISTORY_URL);
							});
				}).onFailure()
				.recoverWithItem(e -> HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
	}

	@Path("/{entityId}")
	@DELETE
	@Counted(name = "temp_entity_delete_total", description = "Total number of temp entity delete requests", absolute = true)
	@Timed(name = "temp_entity_delete_duration", description = "Duration of temp entity delete requests", unit = MetricUnits.MILLISECONDS, absolute = true)
	@ConcurrentGauge(name = "temp_entity_delete_concurrent", description = "Number of concurrent temp entity delete requests", absolute = true)
	public Uni<RestResponse<Object>> deleteTemporalEntityById(HttpServerRequest request,
			@PathParam("entityId") String entityId) {
		try {
			HttpUtils.validateUri(entityId);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
		}
		return ldService.parse(HttpUtils.getAtContext(request)).onItem().transformToUni(ctx -> {
			return historyService.deleteEntry(HttpUtils.getTenant(request), entityId, ctx, request.headers()).onItem()
					.transform(result -> {
						return HttpUtils.generateDeleteResult(result);
					});
		}).onFailure().recoverWithItem(e -> HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
	}

	@Path("/{entityId}/attrs")
	@POST
	@Counted(name = "temp_entity_add_attrs_total", description = "Total number of temp entity add attrs requests", absolute = true)
	@Timed(name = "temp_entity_add_attrs_duration", description = "Duration of temp entity add attrs requests", unit = MetricUnits.MILLISECONDS, absolute = true)
	@ConcurrentGauge(name = "temp_entity_add_attrs_concurrent", description = "Number of concurrent temp entity add attrs requests", absolute = true)
	public Uni<RestResponse<Object>> addAttrib2TemopralEntity(HttpServerRequest request,
			@PathParam("entityId") String entityId, String body) {
		Map<String, Object> payload;

		try {
			payload = new JsonObject(body).getMap();
			HttpUtils.validateUri(entityId);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
		}
		return HttpUtils.expandBody(request, payload, AppConstants.TEMP_ENTITY_UPDATE_PAYLOAD, ldService).onItem()
				.transformToUni(tuple -> {
					return historyService.appendToEntry(HttpUtils.getTenant(request), entityId, tuple.getItem2(),
							tuple.getItem1(), request.headers()).onItem().transform(opResult -> {
								return HttpUtils.generateUpdateResultResponse(opResult);
							});
				}).onFailure()
				.recoverWithItem(e -> HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
	}

	@Path("/{entityId}/attrs/{attrId}")
	@DELETE
	@Counted(name = "temp_entity_delete_attrs_total", description = "Total number of temp entity delete attrs requests", absolute = true)
	@Timed(name = "temp_entity_delete_attrs_duration", description = "Duration of temp entity delete attrs requests", unit = MetricUnits.MILLISECONDS, absolute = true)
	@ConcurrentGauge(name = "temp_entity_delete_attrs_concurrent", description = "Number of concurrent temp entity delete attrs requests", absolute = true)
	public Uni<RestResponse<Object>> deleteAttrib2TemporalEntity(HttpServerRequest request,
			@PathParam("entityId") String entityId, @PathParam("attrId") String attrId,
			@QueryParam("datasetId") String datasetId, @QueryParam("deleteAll") String deleteAllS) {
		boolean deleteAll;
		try {
			HttpUtils.validateUri(entityId);
			deleteAll = HttpUtils.parseBoolean(deleteAllS);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
		}
		return ldService.parse(HttpUtils.getAtContext(request)).onItem().transformToUni(context -> {
			return historyService.deleteAttrFromEntry(HttpUtils.getTenant(request), entityId,
					context.expandIri(attrId, false, true, null, null), datasetId, deleteAll, context,
					request.headers()).onItem().transform(opResult -> {
						return HttpUtils.generateDeleteResult(opResult);
					});
		}).onFailure().recoverWithItem(e -> HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));

	}

	@Path("/{entityId}/attrs/{attrId}/{instanceId}")
	@PATCH
	@Counted(name = "temp_entity_patch_attrs_instance_total", description = "Total number of temp entity attrs instance patch requests", absolute = true)
	@Timed(name = "temp_entity_patch_attrs_instance_duration", description = "Duration of temp entity attrs instance patch requests", unit = MetricUnits.MILLISECONDS, absolute = true)
	@ConcurrentGauge(name = "temp_entity_patch_attrs_instance_concurrent", description = "Number of concurrent temp entity attrs instance patch requests", absolute = true)
	public Uni<RestResponse<Object>> modifyAttribInstanceTemporalEntity(HttpServerRequest request,
			@PathParam("entityId") String entityId, @PathParam("attrId") String attrId,
			@PathParam("instanceId") String instanceId, String body) {
		Map<String, Object> payload;
		try {
			payload = new JsonObject(body).getMap();
			HttpUtils.validateUri(entityId);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
		}

		return HttpUtils.expandBody(request, payload, AppConstants.TEMP_ENTITY_UPDATE_PAYLOAD, ldService).onItem()
				.transformToUni(tuple -> {
					return historyService
							.updateInstanceOfAttr(HttpUtils.getTenant(request), entityId,
									tuple.getItem1().expandIri(attrId, false, true, null, null), instanceId,
									tuple.getItem2(), tuple.getItem1(), request.headers())
							.onItem().transform(opResult -> {
								return HttpUtils.generateUpdateResultResponse(opResult);
							});
				}).onFailure()
				.recoverWithItem(e -> HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));

	}

	@Path("/{entityId}/attrs/{attrId}/{instanceId}")
	@DELETE
	@Counted(name = "temp_entity_delete_attrs_instance_total", description = "Total number of temp entity attrs instance delete requests", absolute = true)
	@Timed(name = "temp_entity_delete_attrs_instance_duration", description = "Duration of temp entity attrs instance delete requests", unit = MetricUnits.MILLISECONDS, absolute = true)
	@ConcurrentGauge(name = "temp_entity_delete_attrs_instance_concurrent", description = "Number of concurrent temp entity attrs instance delete requests", absolute = true)
	public Uni<RestResponse<Object>> deleteAtrribInstanceTemporalEntity(HttpServerRequest request,
			@PathParam("entityId") String entityId, @PathParam("attrId") String attrId,
			@PathParam("instanceId") String instanceId) {
		try {
			HttpUtils.validateUri(entityId);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
		}
		return ldService.parse(HttpUtils.getAtContext(request)).onItem().transformToUni(context -> {
			return historyService
					.deleteInstanceOfAttr(HttpUtils.getTenant(request), entityId,
							context.expandIri(attrId, false, true, null, null), instanceId, context, request.headers())
					.onItem().transform(opResult -> {
						return HttpUtils.generateDeleteResult(opResult);
					});
		}).onFailure().recoverWithItem(e -> HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
	}
}
