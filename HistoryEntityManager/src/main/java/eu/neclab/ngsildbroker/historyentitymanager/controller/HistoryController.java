package eu.neclab.ngsildbroker.historyentitymanager.controller;

import java.util.Map;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.PATCH;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestResponse;

import com.github.jsonldjava.core.JsonLDService;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.historyentitymanager.service.HistoryEntityService;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;

@Singleton
@Path("/ngsi-ld/v1/temporal/entities")
public class HistoryController {

	@Inject
	HistoryEntityService historyService;

	@ConfigProperty(name = "scorpio.history.defaultLimit", defaultValue = "50")
	int defaultLimit;
	@ConfigProperty(name = "scorpio.history.maxLimit", defaultValue = "1000")
	int maxLimit;

	@ConfigProperty(name = "ngsild.corecontext", defaultValue = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld")
	String coreContext;

	@Inject
	JsonLDService ldService;

	@POST
	public Uni<RestResponse<Object>> createTemporalEntity(HttpServerRequest request, Map<String, Object> payload) {
		return HttpUtils.expandBody(request, payload, AppConstants.TEMP_ENTITY_CREATE_PAYLOAD, ldService).onItem()
				.transformToUni(tuple -> {
					return historyService.createEntry(HttpUtils.getTenant(request), tuple.getItem2(), tuple.getItem1(),request.headers())
							.onItem().transform(opResult -> {
								return HttpUtils.generateCreateResult(opResult, AppConstants.HISTORY_URL);
							});
				}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
	}

	@Path("/{entityId}")
	@DELETE
	public Uni<RestResponse<Object>> deleteTemporalEntityById(HttpServerRequest request,
			@PathParam("entityId") String entityId) {
		try {
			HttpUtils.validateUri(entityId);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		return ldService.parse(HttpUtils.getAtContext(request)).onItem().transformToUni(ctx -> {
			return historyService.deleteEntry(HttpUtils.getTenant(request), entityId, ctx,request.headers()).onItem()
					.transform(result -> {
						return HttpUtils.generateDeleteResult(result);
					});
		}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
	}

	@Path("/{entityId}/attrs")
	@POST
	public Uni<RestResponse<Object>> addAttrib2TemopralEntity(HttpServerRequest request,
			@PathParam("entityId") String entityId, Map<String, Object> payload) {
		try {
			HttpUtils.validateUri(entityId);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		return HttpUtils.expandBody(request, payload, AppConstants.TEMP_ENTITY_UPDATE_PAYLOAD, ldService).onItem()
				.transformToUni(tuple -> {
					return historyService
							.appendToEntry(HttpUtils.getTenant(request), entityId, tuple.getItem2(), tuple.getItem1(),request.headers())
							.onItem().transform(opResult -> {
								return HttpUtils.generateUpdateResultResponse(opResult);
							});
				}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
	}

	@Path("/{entityId}/attrs/{attrId}")
	@DELETE
	public Uni<RestResponse<Object>> deleteAttrib2TemporalEntity(HttpServerRequest request,
			@PathParam("entityId") String entityId, @PathParam("attrId") String attrId,
			@QueryParam("datasetId") String datasetId, @QueryParam("deleteAll") boolean deleteAll) {
		try {
			HttpUtils.validateUri(entityId);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		return ldService.parse(HttpUtils.getAtContext(request)).onItem().transformToUni(context -> {
			return historyService
					.deleteAttrFromEntry(HttpUtils.getTenant(request), entityId,
							context.expandIri(attrId, false, true, null, null), datasetId, deleteAll, context,request.headers())
					.onItem().transform(opResult -> {
						return HttpUtils.generateDeleteResult(opResult);
					});
		}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);

	}

	@Path("/{entityId}/attrs/{attrId}/{instanceId}")
	@PATCH
	public Uni<RestResponse<Object>> modifyAttribInstanceTemporalEntity(HttpServerRequest request,
			@PathParam("entityId") String entityId, @PathParam("attrId") String attrId,
			@PathParam("instanceId") String instanceId, Map<String, Object> payload) {
		try {
			HttpUtils.validateUri(entityId);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}

		return HttpUtils.expandBody(request, payload, AppConstants.TEMP_ENTITY_UPDATE_PAYLOAD, ldService).onItem()
				.transformToUni(tuple -> {
					return historyService.updateInstanceOfAttr(HttpUtils.getTenant(request), entityId,
							tuple.getItem1().expandIri(attrId, false, true, null, null), instanceId, tuple.getItem2(),
							tuple.getItem1(),request.headers()).onItem().transform(opResult -> {
								return HttpUtils.generateUpdateResultResponse(opResult);
							});
				}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);

	}

	@Path("/{entityId}/attrs/{attrId}/{instanceId}")
	@DELETE
	public Uni<RestResponse<Object>> deleteAtrribInstanceTemporalEntity(HttpServerRequest request,
			@PathParam("entityId") String entityId, @PathParam("attrId") String attrId,
			@PathParam("instanceId") String instanceId) {
		try {
			HttpUtils.validateUri(entityId);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		return ldService.parse(HttpUtils.getAtContext(request)).onItem().transformToUni(context -> {
			return historyService
					.deleteInstanceOfAttr(HttpUtils.getTenant(request), entityId,
							context.expandIri(attrId, false, true, null, null), instanceId, context,request.headers())
					.onItem().transform(opResult -> {
						return HttpUtils.generateDeleteResult(opResult);
					});
		}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
	}
}
