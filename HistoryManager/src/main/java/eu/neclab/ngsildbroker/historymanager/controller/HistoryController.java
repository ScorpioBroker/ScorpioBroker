package eu.neclab.ngsildbroker.historymanager.controller;

import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PATCH;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
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
import eu.neclab.ngsildbroker.historymanager.service.HistoryService;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;

@Path("/ngsi-ld/v1/temporal/entities")
public class HistoryController {

	private final static Logger logger = LoggerFactory.getLogger(HistoryController.class);

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
	void init() {
		JsonLdProcessor.init(coreContext);
	}

	@POST
	public Uni<RestResponse<Object>> createTemporalEntity(HttpServerRequest request, String payload) {
		return EntryControllerFunctions.createEntry(historyService, request, payload,
				AppConstants.TEMP_ENTITY_CREATE_PAYLOAD, AppConstants.HISTORY_URL, logger);
	}

	@GET
	public Uni<RestResponse<Object>> retrieveTemporalEntity(HttpServerRequest request) {
		return QueryControllerFunctions.queryForEntries(historyService, request, true, defaultLimit, maxLimit, true);
	}

	@Path("/{entityId}")
	@GET
	public Uni<RestResponse<Object>> retrieveTemporalEntityById(HttpServerRequest request, String entityId) {
		return QueryControllerFunctions.getEntity(historyService, request, null, null, entityId, true, defaultLimit,
				maxLimit);

	}

	@Path("/{entityId}")
	@DELETE
	public RestResponse<Object> deleteTemporalEntityById(HttpServerRequest request, String entityId) {
		return EntryControllerFunctions.deleteEntry(historyService, request, entityId, logger);

	}

	@Path("/{entityId}/attrs")
	@POST
	public RestResponse<Object> addAttrib2TemopralEntity(HttpServerRequest request, String entityId, String payload,
			@QueryParam(value = "options") String options) {
		return EntryControllerFunctions.appendToEntry(historyService, request, entityId, payload, options,
				AppConstants.TEMP_ENTITY_UPDATE_PAYLOAD, logger);
	}

	@Path("/{entityId}/attrs/{attrId}")
	@DELETE
	public RestResponse<Object> deleteAttrib2TemporalEntity(HttpServerRequest request, String entityId, String attrId) {
		try {
			HttpUtils.validateUri(entityId);
			Context context = JsonLdProcessor.getCoreContextClone();
			List<Object> links = HttpUtils.getAtContext(request);
			context = context.parse(links, true);
			logger.trace("deleteAttrib2TemporalEntity :: started");
			logger.debug("entityId : " + entityId + " attrId : " + attrId);
			historyService.delete(HttpUtils.getHeaders(request), entityId, attrId, null, context);
			logger.trace("deleteAttrib2TemporalEntity :: completed");
			return RestResponse.noContent();
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
	}

	@Path("/{entityId}/attrs/{attrId}/{instanceId}")
	@PATCH
	public RestResponse<Object> modifyAttribInstanceTemporalEntity(HttpServerRequest request, String entityId,
			String attrId, String instanceId, String payload) {
		try {
			HttpUtils.validateUri(entityId);
			HttpUtils.validateUri(instanceId);
			logger.trace("modifyAttribInstanceTemporalEntity :: started");
			logger.debug("entityId : " + entityId + " attrId : " + attrId + " instanceId : " + instanceId);
			Context context = JsonLdProcessor.getCoreContextClone();

			List<Object> linkHeaders = HttpUtils.getAtContext(request);
			boolean atContextAllowed = HttpUtils.doPreflightCheck(request, linkHeaders);
			context = context.parse(linkHeaders, true);

			@SuppressWarnings("unchecked")
			Map<String, Object> resolved = (Map<String, Object>) JsonLdProcessor.expand(linkHeaders,
					JsonUtils.fromString(payload), opts, AppConstants.TEMP_ENTITY_UPDATE_PAYLOAD, atContextAllowed)
					.get(0);
			// TODO : TBD- conflict between specs and implementation <mentioned no request
			// body in specs>
			historyService.modifyAttribInstanceTemporalEntity(HttpUtils.getHeaders(request), entityId, resolved, attrId,
					instanceId, context);
			logger.trace("modifyAttribInstanceTemporalEntity :: completed");
			return RestResponse.noContent();
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
	}

	@Path("/{entityId}/attrs/{attrId}/{instanceId}")
	@DELETE
	public RestResponse<Object> deleteAtrribInstanceTemporalEntity(HttpServerRequest request, String entityId,
			String attrId, String instanceId) {
		try {
			logger.trace("deleteAtrribInstanceTemporalEntity :: started");
			HttpUtils.validateUri(entityId);
			HttpUtils.validateUri(instanceId);
			logger.debug("entityId : " + entityId + " attrId : " + attrId + " instanceId : " + instanceId);
			Context context = JsonLdProcessor.getCoreContextClone();
			List<Object> links = HttpUtils.getAtContext(request);
			context = context.parse(links, true);

			historyService.delete(HttpUtils.getHeaders(request), entityId, attrId, instanceId, context);
			logger.trace("deleteAtrribInstanceTemporalEntity :: completed");
			return RestResponse.noContent();
		} catch (Exception exception) {
			return HttpUtils.handleControllerExceptions(exception);
		}
	}
}
