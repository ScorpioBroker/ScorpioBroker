package eu.neclab.ngsildbroker.historyentitymanager.controller;

import java.util.Map;
import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.DELETE;
import javax.ws.rs.PATCH;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestResponse;
import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdProcessor;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.historyentitymanager.service.HistoryEntityService;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.core.http.HttpServerRequest;

@Singleton
@Path("/ngsi-ld/v1/temporal/entities")
public class HistoryController {

	@Inject
	HistoryEntityService historyService;
	@ConfigProperty(name = "atcontext.url")
	String atContextServerUrl;
	@ConfigProperty(name = "scorpio.history.defaultLimit", defaultValue = "50")
	int defaultLimit;
	@ConfigProperty(name = "scorpio.history.maxLimit", defaultValue = "1000")
	int maxLimit;

	@ConfigProperty(name = "ngsild.corecontext", defaultValue = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld")
	String coreContext;

	@PostConstruct
	public void init() {
		JsonLdProcessor.init(coreContext);
	}

	@POST
	public Uni<RestResponse<Object>> createTemporalEntity(HttpServerRequest request, String payload) {
		Tuple2<Context, Map<String, Object>> tuple;
		try {
			tuple = HttpUtils.expandBody(request, payload, AppConstants.TEMP_ENTITY_CREATE_PAYLOAD);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		return historyService.createEntry(HttpUtils.getTenant(request), tuple.getItem2(), tuple.getItem1()).onItem()
				.transform(opResult -> {
					return HttpUtils.generateCreateResult(opResult, AppConstants.HISTORY_URL);
				}).onFailure().recoverWithItem(error -> {
					return HttpUtils.handleControllerExceptions(error);
				});
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
		return historyService
				.deleteEntry(HttpUtils.getTenant(request), entityId,
						JsonLdProcessor.getCoreContextClone().parse(HttpUtils.getAtContext(request), true))
				.onItem().transform(result -> {
					return HttpUtils.generateDeleteResult(result);
				}).onFailure().recoverWithItem(error -> {
					return HttpUtils.handleControllerExceptions(error);
				});

	}

	@Path("/{entityId}/attrs")
	@POST
	public Uni<RestResponse<Object>> addAttrib2TemopralEntity(HttpServerRequest request,
			@PathParam("entityId") String entityId, String payload) {
		Tuple2<Context, Map<String, Object>> tuple;
		try {
			HttpUtils.validateUri(entityId);
			tuple = HttpUtils.expandBody(request, payload, AppConstants.TEMP_ENTITY_UPDATE_PAYLOAD);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}

		return historyService.appendToEntry(HttpUtils.getTenant(request), entityId, tuple.getItem2(), tuple.getItem1())
				.onItem().transform(opResult -> {
					return HttpUtils.generateUpdateResultResponse(opResult);
				}).onFailure().recoverWithItem(error -> {
					return HttpUtils.handleControllerExceptions(error);
				});
	}

	@Path("/{entityId}/attrs/{attrId}")
	@DELETE
	public Uni<RestResponse<Object>> deleteAttrib2TemporalEntity(HttpServerRequest request,
			@PathParam("entityId") String entityId, @PathParam("attrId") String attrId,
			@QueryParam("datasetId") String datasetId, @QueryParam("deleteAll") boolean deleteAll) {
		Context context;
		try {
			HttpUtils.validateUri(entityId);
			context = JsonLdProcessor.getCoreContextClone().parse(HttpUtils.getAtContext(request), true);
			attrId = context.expandIri(attrId, false, true, null, null);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		return historyService
				.deleteAttrFromEntry(HttpUtils.getTenant(request), entityId, attrId, datasetId, deleteAll, context)
				.onItem().transform(opResult -> {
					return HttpUtils.generateDeleteResult(opResult);
				}).onFailure().recoverWithItem(error -> {
					return HttpUtils.handleControllerExceptions(error);
				});

	}

	@Path("/{entityId}/attrs/{attrId}/{instanceId}")
	@PATCH
	public Uni<RestResponse<Object>> modifyAttribInstanceTemporalEntity(HttpServerRequest request,
			@PathParam("entityId") String entityId, @PathParam("attrId") String attrId,
			@PathParam("instanceId") String instanceId, String payload) {
		Tuple2<Context, Map<String, Object>> tuple;
		try {
			HttpUtils.validateUri(entityId);
			tuple = HttpUtils.expandBody(request, payload, AppConstants.TEMP_ENTITY_UPDATE_PAYLOAD);
			attrId = tuple.getItem1().expandIri(attrId, false, false, null, null);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		return historyService.updateInstanceOfAttr(HttpUtils.getTenant(request), entityId, attrId, instanceId,
				tuple.getItem2(), tuple.getItem1()).onItem().transform(opResult -> {
					return HttpUtils.generateUpdateResultResponse(opResult);
				}).onFailure().recoverWithItem(error -> {
					return HttpUtils.handleControllerExceptions(error);
				});

	}

	@Path("/{entityId}/attrs/{attrId}/{instanceId}")
	@DELETE
	public Uni<RestResponse<Object>> deleteAtrribInstanceTemporalEntity(HttpServerRequest request,
			@PathParam("entityId") String entityId, @PathParam("attrId") String attrId,
			@PathParam("instanceId") String instanceId) {
		Context context;
		try {
			HttpUtils.validateUri(entityId);
			context = JsonLdProcessor.getCoreContextClone().parse(HttpUtils.getAtContext(request), true);
			attrId = context.expandIri(attrId, false, false, null, null);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e));
		}
		return historyService.deleteInstanceOfAttr(HttpUtils.getTenant(request), entityId, attrId, instanceId, context)
				.onItem().transform(opResult -> {
					return HttpUtils.generateDeleteResult(opResult);
				}).onFailure().recoverWithItem(error -> {
					return HttpUtils.handleControllerExceptions(error);
				});
	}
}
