package eu.neclab.ngsildbroker.historymanager.controller;

import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PATCH;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.resteasy.reactive.RestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.controllers.EntryControllerFunctions;
import eu.neclab.ngsildbroker.commons.controllers.QueryControllerFunctions;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.historymanager.repository.HistoryDAO;
import eu.neclab.ngsildbroker.historymanager.service.HistoryService;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;

@Singleton
@Path("/ngsi-ld/v1/temporal/entities")
public class HistoryController {

	private final static Logger logger = LoggerFactory.getLogger(HistoryController.class);

	@Inject
	HistoryDAO historyDAO;
	@Inject
	HistoryService historyService;
	@ConfigProperty(name = "atcontext.url")
	String atContextServerUrl;
	@ConfigProperty(name = "scorpio.history.defaultLimit", defaultValue = "50")
	int defaultLimit;
	@ConfigProperty(name = "scorpio.history.maxLimit", defaultValue = "1000")
	int maxLimit;

	private JsonLdOptions opts = new JsonLdOptions(JsonLdOptions.JSON_LD_1_1);

	@ConfigProperty(name = "ngsild.corecontext", defaultValue = "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld")
	String coreContext;

	@PostConstruct
	public void init() {
		JsonLdProcessor.init(coreContext);
	}

	@POST
	public Uni<RestResponse<Object>> createTemporalEntity(HttpServerRequest request, String payload) {
		return EntryControllerFunctions.createEntry(historyService, request, payload,
				AppConstants.TEMP_ENTITY_CREATE_PAYLOAD, AppConstants.HISTORY_URL, logger, false);
	}

	@GET
	public Uni<RestResponse<Object>> retrieveTemporalEntity(HttpServerRequest request,
			@QueryParam(value = "limit") Integer limit, @QueryParam(value = "offset") Integer offset,
			@QueryParam(value = "qtoken") String qToken, @QueryParam(value = "options") List<String> options,
			@QueryParam(value = "count") Boolean countResult) {
		return QueryControllerFunctions.queryForEntries(historyService, request, true, defaultLimit, maxLimit, true);
	}

	@Path("/{entityId}")
	@GET
	public Uni<RestResponse<Object>> retrieveTemporalEntityById(HttpServerRequest request,
			@PathParam("entityId") String entityId) {
		return QueryControllerFunctions.getEntity(historyService, request, null, null, entityId, true, defaultLimit,
				maxLimit);

	}

	@Path("/{entityId}")
	@DELETE
	public Uni<RestResponse<Object>> deleteTemporalEntityById(HttpServerRequest request,
			@PathParam("entityId") String entityId) {
		return EntryControllerFunctions.deleteEntry(historyService, request, entityId, logger);

	}

	@Path("/{entityId}/attrs")
	@POST
	public Uni<RestResponse<Object>> addAttrib2TemopralEntity(HttpServerRequest request,
			@PathParam("entityId") String entityId, String payload, @QueryParam(value = "options") String options) {
		return EntryControllerFunctions.appendToEntry(historyService, request, entityId, payload, options,
				AppConstants.TEMP_ENTITY_UPDATE_PAYLOAD, logger, false);
	}

	@Path("/{entityId}/attrs/{attrId}")
	@DELETE
	public Uni<RestResponse<Object>> deleteAttrib2TemporalEntity(HttpServerRequest request,
			@PathParam("entityId") String entityId, @PathParam("attrId") String attrId) {
		return Uni.combine().all().unis(HttpUtils.validateUri(entityId), HttpUtils.getAtContext(request)).asTuple()
				.onItem().transformToUni(t -> {
					Context context = JsonLdProcessor.getCoreContextClone();
					List<Object> links = t.getItem2();
					context = context.parse(links, true);

					logger.trace("deleteAttrib2TemporalEntity :: started");
					logger.debug("entityId : " + entityId + " attrId : " + attrId);

					return historyService.delete(HttpUtils.getInternalTenant(request), entityId,
							attrId, null, context).onItem().transform(t2 -> {
								logger.trace("deleteAttrib2TemporalEntity :: completed");
								return RestResponse.noContent();
							});
				}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
	}

	@Path("/{entityId}/attrs/{attrId}/{instanceId}")
	@PATCH
	public Uni<RestResponse<Object>> modifyAttribInstanceTemporalEntity(HttpServerRequest request,
			@PathParam("entityId") String entityId, @PathParam("attrId") String attrId,
			@PathParam("instanceId") String instanceId, String payload) {
		return Uni.combine().all().unis(HttpUtils.validateUri(entityId), HttpUtils.validateUri(instanceId),
				HttpUtils.getAtContext(request)).asTuple().onItem().transformToUni(t -> {

					logger.trace("modifyAttribInstanceTemporalEntity :: started");
					logger.debug("entityId : " + entityId + " attrId : " + attrId + " instanceId : " + instanceId);
					Context context = JsonLdProcessor.getCoreContextClone();
					List<Object> linkHeaders = t.getItem3();
					context = context.parse(linkHeaders, true);

					boolean atContextAllowed;
					try {
						atContextAllowed = HttpUtils.doPreflightCheck(request, linkHeaders);
						@SuppressWarnings("unchecked")
						Map<String, Object> resolved = (Map<String, Object>) JsonLdProcessor
								.expand(linkHeaders, JsonUtils.fromString(payload), opts,
										AppConstants.TEMP_ENTITY_UPDATE_PAYLOAD, atContextAllowed)
								.get(0);
						return historyService.modifyAttribInstanceTemporalEntity(
								HttpUtils.getInternalTenant(request), entityId, resolved, attrId,
								instanceId, context).onItem().transform(t2 -> {
									return RestResponse.noContent();
								}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);

					} catch (Exception exception) {
						return Uni.createFrom().item(HttpUtils.handleControllerExceptions(exception));
					}
				}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
	}

	@Path("/{entityId}/attrs/{attrId}/{instanceId}")
	@DELETE
	public Uni<RestResponse<Object>> deleteAtrribInstanceTemporalEntity(HttpServerRequest request,
			@PathParam("entityId") String entityId, @PathParam("attrId") String attrId,
			@PathParam("instanceId") String instanceId) {
		return Uni.combine().all().unis(HttpUtils.validateUri(entityId), HttpUtils.validateUri(instanceId),
				HttpUtils.getAtContext(request)).asTuple().onItem().transformToUni(t -> {
					logger.debug("entityId : " + entityId + " attrId : " + attrId + " instanceId : " + instanceId);

					Context context = JsonLdProcessor.getCoreContextClone();
					List<Object> links = t.getItem3();
					context = context.parse(links, true);

					return historyService.delete(HttpUtils.getInternalTenant(request), entityId,
							attrId, instanceId, context).onItem().transform(t2 -> {
								logger.trace("deleteAtrribInstanceTemporalEntity :: completed");
								return RestResponse.noContent();

							});

				}).onFailure().recoverWithItem(HttpUtils::handleControllerExceptions);
	}
}
